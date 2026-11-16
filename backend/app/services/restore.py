"""检查点恢复数据清单：恢复准入、唯一恢复锁、路径引用与状态流转。

这一层是检查点恢复与流作业运维共用的唯一事实来源：
- 检查点入口发起恢复时，读取 CHEC 记录的检查点ID、保存路径、状态大小并校验恢复条件；
- 同一检查点只发放一把恢复锁，同一恢复请求号重复提交返回同一份清单；
- 保存路径用独立引用账本登记，恢复失败或归档时清单与路径引用一起释放；
- 流作业运维入口直接读取同一份清单，并维护自己的消费处理记录。
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

from app.services.locks import RESTORE_MUTEX
from app.store import store

MANIFEST_MODULE = "restore_manifests"
PATH_MODULE = "restore_paths"
ROLLBACK_MODULE = "restore_rollbacks"
CONSUMPTION_MODULE = "restore_consumptions"

STATUS_PENDING = "待处理"
STATUS_RUNNING = "运行中"
STATUS_RESTORED = "已恢复"
STATUS_ARCHIVED = "已归档"
STATUS_ROLLED_BACK = "已回退"

MANIFEST_STATUS_ORDER = [STATUS_PENDING, STATUS_RUNNING, STATUS_RESTORED, STATUS_ARCHIVED]
ACTIVE_STATUSES = {STATUS_PENDING, STATUS_RUNNING, STATUS_RESTORED}
TERMINAL_STATUSES = {STATUS_ARCHIVED, STATUS_ROLLED_BACK}

ACTION_TARGETS = {
    "开始恢复": STATUS_RUNNING,
    "完成恢复": STATUS_RESTORED,
    "归档恢复": STATUS_ARCHIVED,
    "恢复失败": STATUS_ROLLED_BACK,
}
TRANSITION_RULES = {
    STATUS_PENDING: {STATUS_RUNNING, STATUS_ROLLED_BACK},
    STATUS_RUNNING: {STATUS_RESTORED, STATUS_ROLLED_BACK},
    STATUS_RESTORED: {STATUS_ARCHIVED},
}

JOB_ACTIVE_STATUSES = {"待启动", "运行中", "已暂停"}


class RestoreError(Exception):
    """恢复准入失败：由调用方翻译成可读提示，不写入任何清单。"""


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _pick(values: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in values and values[name] not in (None, ""):
            return values[name]
    return None


def _text(value: Any) -> str:
    return str(value or "").strip()


def _parse_seconds(value: Any, fallback: Any = 0) -> int:
    """把「42」「42 秒」「42s」都归一成整数秒，非法值抛 RestoreError。"""
    raw = value if value not in (None, "") else fallback
    if raw in (None, ""):
        return 0
    match = re.search(r"-?\d+", str(raw))
    if match is None:
        raise RestoreError("恢复耗时必须是秒数，例如 42")
    seconds = int(match.group())
    if seconds < 0:
        raise RestoreError("恢复耗时不能为负数")
    return seconds


def _next_id(module: str) -> int:
    rows = store.rows(module)
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _next_job_number() -> str:
    numbers = []
    for row in store.rows("jobops"):
        match = re.search(r"(\d+)$", _text(row.get("实例编号")))
        if match:
            numbers.append(int(match.group(1)))
    return f"JOBO-{max(numbers, default=0) + 1:04d}"


def _new_lock() -> str:
    existing = {_text(row.get("恢复锁")) for row in store.rows(MANIFEST_MODULE)}
    while True:
        lock = f"LOCK-{uuid.uuid4().hex[:12].upper()}"
        if lock not in existing:
            return lock


def find_manifest(restore_lock: str) -> dict[str, Any] | None:
    for row in store.rows(MANIFEST_MODULE):
        if _text(row.get("恢复锁")) == _text(restore_lock):
            return row
    return None


def find_manifest_by_request(request_no: str) -> dict[str, Any] | None:
    for row in store.rows(MANIFEST_MODULE):
        if _text(row.get("恢复请求号")) == _text(request_no):
            return row
    return None


def find_consumption(restore_lock: str) -> dict[str, Any] | None:
    for row in store.rows(CONSUMPTION_MODULE):
        if _text(row.get("恢复锁")) == _text(restore_lock):
            return row
    return None


def public_view(row: dict[str, Any]) -> dict[str, Any]:
    """对外返回清单时去掉下划线开头的内部字段（例如回退用的实例快照）。"""
    return {key: value for key, value in row.items() if not key.startswith("_")}


def list_manifests(
    *,
    keyword: str | None = None,
    status: str | None = None,
    page: int = 1,
    size: int = 20,
) -> tuple[list[dict[str, Any]], int]:
    rows = store.rows(MANIFEST_MODULE)
    if keyword:
        fields = ("恢复请求号", "记录编号", "检查点ID", "保存路径", "目标实例", "恢复锁")
        rows = [row for row in rows if any(keyword in _text(row.get(field)) for field in fields)]
    if status:
        rows = [row for row in rows if _text(row.get("状态")) == status]
    total = len(rows)
    start = max(page - 1, 0) * size
    return [public_view(row) for row in rows[start:start + size]], total


def list_rollbacks(*, page: int = 1, size: int = 50) -> tuple[list[dict[str, Any]], int]:
    rows = store.rows(ROLLBACK_MODULE)
    total = len(rows)
    start = max(page - 1, 0) * size
    return rows[start:start + size], total


def _path_rows(path: str) -> list[dict[str, Any]]:
    return [row for row in store.rows(PATH_MODULE) if _text(row.get("保存路径")) == path]


def _release_path(restore_lock: str) -> None:
    rows = store.rows(PATH_MODULE)
    store.replace_rows(PATH_MODULE, [
        row for row in rows if _text(row.get("恢复锁")) != _text(restore_lock)
    ])


def _sync_consumption(restore_lock: str, manifest_status: str, note: str) -> None:
    record = find_consumption(restore_lock)
    if record is None:
        return
    record["清单状态"] = manifest_status
    record["处理记录"] = note
    if manifest_status in TERMINAL_STATUSES:
        record["消费状态"] = "已失效" if manifest_status == STATUS_ROLLED_BACK else "已归档"
        record["清单更新时间"] = _now()


def _instance_path(instance: dict[str, Any]) -> str:
    return _text(instance.get("保存路径"))


def _instance_checkpoint(instance: dict[str, Any]) -> str:
    return _text(instance.get("检查点ID"))


def _instance_number(instance: dict[str, Any]) -> str:
    return _text(instance.get("实例编号"))


def _set_instance_status(instance: dict[str, Any], status: str) -> None:
    instance["status"] = status
    instance["实例状态"] = status
    instance["pending"] = status in JOB_ACTIVE_STATUSES


def _snapshot_instance(instance: dict[str, Any]) -> dict[str, Any]:
    return {
        "实例编号": _instance_number(instance),
        "status": instance.get("status"),
        "实例状态": instance.get("实例状态"),
        "pending": instance.get("pending"),
        "abnormal": instance.get("abnormal"),
    }


INSTANCE_SNAPSHOT_FIELDS = (
    "实例编号",
    "作业名称",
    "status",
    "实例状态",
    "pending",
    "abnormal",
    "检查点ID",
    "保存路径",
    "恢复请求号",
    "恢复锁",
    "superseded_by",
)


def _store_instance(number: str) -> dict[str, Any] | None:
    for instance in store.rows("jobops"):
        if _instance_number(instance) == number:
            return instance
    return None


def _snapshot_instance_fields(instance: dict[str, Any] | None) -> dict[str, Any] | None:
    if instance is None:
        return None
    return {
        field: instance.get(field)
        for field in INSTANCE_SNAPSHOT_FIELDS
        if field in instance
    }


def _restore_instance_fields(snapshot: dict[str, Any]) -> None:
    """把复用实例恢复到准入前的字段状态，避免留下半套恢复绑定。"""
    instance = _store_instance(_text(snapshot.get("实例编号")))
    if instance is None:
        return
    for field in INSTANCE_SNAPSHOT_FIELDS:
        if field in snapshot:
            instance[field] = snapshot[field]
        else:
            instance.pop(field, None)


def _restore_instance_snapshot(snapshot: dict[str, Any]) -> None:
    number = _text(snapshot.get("实例编号"))
    for instance in store.rows("jobops"):
        if _instance_number(instance) != number:
            continue
        instance["status"] = snapshot.get("status")
        instance["实例状态"] = snapshot.get("实例状态")
        instance["pending"] = snapshot.get("pending")
        instance["abnormal"] = snapshot.get("abnormal")
        return


def _retire_old_instances(
    checkpoint_id: str,
    save_path: str,
    target_number: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """退役同一检查点下仍占用保存路径的旧实例，返回（旧实例编号, 原状态快照）。

    不同检查点却占用同一保存路径的实例无法被自动判定为旧实例，直接拒绝恢复，
    避免出现两个实例同时持有一份检查点保存路径。
    """
    retired: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    for instance in store.rows("jobops"):
        if _instance_number(instance) == target_number:
            continue
        if _instance_path(instance) != save_path:
            continue
        if _text(instance.get("status")) not in JOB_ACTIVE_STATUSES:
            continue
        if _instance_checkpoint(instance) != checkpoint_id:
            raise RestoreError(
                f"保存路径 {save_path} 已被实例 {_instance_number(instance)} 占用，不能重复启动"
            )
        snapshots.append(_snapshot_instance(instance))
        retired.append({"实例编号": _instance_number(instance), "原状态": instance.get("status")})
        _set_instance_status(instance, "已停止")
        instance["superseded_by"] = target_number
    return retired, snapshots


def _ensure_target_instance(
    *,
    checkpoint: dict[str, Any],
    save_path: str,
    target_number: str,
    restore_lock: str,
    request_no: str,
    run_node: str,
) -> tuple[dict[str, Any], bool]:
    checkpoint_id = _text(checkpoint.get("检查点ID"))
    for instance in store.rows("jobops"):
        if _instance_number(instance) != target_number:
            continue
        bound_checkpoint = _instance_checkpoint(instance)
        bound_path = _instance_path(instance)
        current_status = _text(instance.get("status"))
        if bound_path and bound_path != save_path:
            raise RestoreError(f"目标实例 {target_number} 已绑定保存路径 {bound_path}，不能改为 {save_path}")
        if bound_checkpoint and bound_checkpoint != checkpoint_id:
            raise RestoreError(f"目标实例 {target_number} 已绑定检查点 {bound_checkpoint}，不能重复恢复")
        if current_status in {"运行中", "已暂停"}:
            raise RestoreError(f"目标实例 {target_number} 当前为{current_status}，不能作为新的恢复目标")
        instance["检查点ID"] = checkpoint_id
        instance["保存路径"] = save_path
        instance["恢复请求号"] = request_no
        instance["恢复锁"] = restore_lock
        instance["作业名称"] = instance.get("作业名称") or _text(checkpoint.get("流作业名")) or checkpoint_id
        _set_instance_status(instance, "待启动")
        return instance, False

    rows = store.rows("jobops")
    instance = {
        "id": _next_id("jobops"),
        "status": "待启动",
        "pending": True,
        "abnormal": False,
        "实例编号": target_number,
        "作业名称": _text(checkpoint.get("流作业名")) or checkpoint_id,
        "运行节点": run_node,
        "启动时间": "-",
        "运行时长": "-",
        "异常次数": 0,
        "重启次数": 0,
        "实例状态": "待启动",
        "检查点ID": checkpoint_id,
        "保存路径": save_path,
        "恢复请求号": request_no,
        "恢复锁": restore_lock,
    }
    rows.append(instance)
    return instance, True


def _resolve_target_number(values: dict[str, Any], checkpoint: dict[str, Any]) -> str:
    explicit = _text(_pick(values, "目标实例", "target_instance", "target"))
    if explicit:
        return explicit
    checkpoint_id = _text(checkpoint.get("检查点ID"))
    bound = [
        instance for instance in store.rows("jobops")
        if _instance_checkpoint(instance) == checkpoint_id
    ]
    # 先复用还没启动的目标实例；没有再复用同一检查点下已停止的旧实例，
    # 这样重复恢复不会另起一个持有同一保存路径的实例。
    for wanted in ("待启动", "已停止"):
        for instance in bound:
            if _text(instance.get("status")) == wanted:
                return _instance_number(instance)
    return _next_job_number()


def admit_restore(entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """从指定 CHEC 记录发起恢复，成功时写入唯一一份恢复清单。"""
    with RESTORE_MUTEX:
        checkpoint = store.find("checkpoint", entry_id)
        if checkpoint is None:
            return None, f"检查点记录 {entry_id} 不存在或已归档"

        request_no = _text(_pick(values, "恢复请求号", "restore_id", "request_id"))
        if not request_no:
            return None, "缺少必填字段：恢复请求号"

        existing = find_manifest_by_request(request_no)
        if existing is not None:
            # 重复提交：只要落在同一检查点上就返回同一份清单，不再新建实例或锁。
            if _text(existing.get("记录编号")) != _text(checkpoint.get("记录编号")):
                return None, f"恢复请求号 {request_no} 已用于检查点 {existing.get('记录编号')}，不能重复使用"
            same_lock = _text(existing.get("恢复锁")) == _text(checkpoint.get("恢复锁"))
            same_target = _text(existing.get("目标实例")) == _text(checkpoint.get("目标实例"))
            if not same_lock or not same_target:
                return None, f"检查点 {checkpoint.get('检查点ID')} 的恢复请求号已变化，请先用新的恢复请求号重新发起"
            return existing, f"恢复请求 {request_no} 已提交，返回已有恢复清单"

        checkpoint_id = _text(checkpoint.get("检查点ID"))
        save_path = _text(checkpoint.get("保存路径"))
        state_size = _text(checkpoint.get("状态大小"))
        missing = [
            label
            for label, value in (("检查点ID", checkpoint_id), ("保存路径", save_path), ("状态大小", state_size))
            if not value
        ]
        if missing:
            return None, f"检查点记录缺少恢复所需字段：{'、'.join(missing)}"
        if _text(checkpoint.get("status")) in {"已过期", "已清理"}:
            return None, f"检查点记录当前为{checkpoint.get('status')}，不满足恢复条件"

        try:
            seconds = _parse_seconds(_pick(values, "恢复耗时", "restore_seconds", "duration"), checkpoint.get("恢复耗时"))
        except RestoreError as error:
            return None, str(error)

        locked = [
            row for row in store.rows(MANIFEST_MODULE)
            if _text(row.get("记录编号")) == _text(checkpoint.get("记录编号"))
            and _text(row.get("状态")) in ACTIVE_STATUSES
        ]
        if locked:
            holder = locked[0]
            return None, (
                f"检查点 {checkpoint_id} 已由恢复请求 {holder.get('恢复请求号')} 锁定"
                f"（恢复锁 {holder.get('恢复锁')}），同一检查点只允许一个恢复实例"
            )

        path_holders = _path_rows(save_path)
        if path_holders:
            holder = path_holders[0]
            return None, (
                f"保存路径 {save_path} 已被恢复请求 {holder.get('恢复请求号')} 占用"
                f"（恢复锁 {holder.get('恢复锁')}），不能重复启动"
            )

        target_number = _resolve_target_number(values, checkpoint)
        restore_lock = _new_lock()
        run_node = _text(_pick(values, "运行节点", "run_node")) or "恢复节点-01"

        manifest: dict[str, Any] = {}
        path_row: dict[str, Any] = {}
        created_instance = False
        target_instance: dict[str, Any] | None = None
        previous_instance_values: dict[str, Any] | None = None
        retired: list[dict[str, Any]] = []
        snapshots: list[dict[str, Any]] = []
        try:
            retired, snapshots = _retire_old_instances(checkpoint_id, save_path, target_number)
            if _store_instance(target_number) is not None:
                previous_instance_values = _snapshot_instance_fields(_store_instance(target_number))
            target_instance, created_instance = _ensure_target_instance(
                checkpoint=checkpoint,
                save_path=save_path,
                target_number=target_number,
                restore_lock=restore_lock,
                request_no=request_no,
                run_node=run_node,
            )
            manifest = {
                "id": _next_id(MANIFEST_MODULE),
                "状态": STATUS_PENDING,
                "status": STATUS_PENDING,
                "pending": True,
                "abnormal": False,
                "恢复请求号": request_no,
                "恢复锁": restore_lock,
                "恢复锁状态": "已占用",
                "记录编号": checkpoint.get("记录编号"),
                "检查点ID": checkpoint_id,
                "保存路径": save_path,
                "路径引用": "已占用",
                "状态大小": state_size,
                "目标实例": target_number,
                "_目标实例准入前": previous_instance_values,
                "旧实例": [item["实例编号"] for item in retired],
                "旧实例明细": snapshots,
                "恢复耗时": seconds,
                "恢复耗时单位": "秒",
                "目标实例来源": "新建" if created_instance else "复用",
                "创建时间": _now(),
                "更新时间": _now(),
            }
            path_row = {
                "id": _next_id(PATH_MODULE),
                "保存路径": save_path,
                "恢复锁": restore_lock,
                "恢复请求号": request_no,
                "记录编号": checkpoint.get("记录编号"),
                "目标实例": target_number,
                "状态": "占用",
                "占用时间": _now(),
            }
            store.rows(MANIFEST_MODULE).append(manifest)
            store.rows(PATH_MODULE).append(path_row)
        except Exception:
            # 准入阶段任何一步失败都一起退回：清单、路径引用、目标实例与旧实例状态。
            store.replace_rows(
                MANIFEST_MODULE,
                [row for row in store.rows(MANIFEST_MODULE) if row is not manifest],
            )
            store.replace_rows(
                PATH_MODULE,
                [row for row in store.rows(PATH_MODULE) if row is not path_row],
            )
            if created_instance:
                store.replace_rows(
                    "jobops",
                    [row for row in store.rows("jobops") if _instance_number(row) != target_number],
                )
            elif previous_instance_values is not None:
                _restore_instance_fields(previous_instance_values)
            for snapshot in snapshots:
                _restore_instance_snapshot(snapshot)
            raise

        checkpoint["恢复请求号"] = request_no
        checkpoint["恢复锁"] = restore_lock
        checkpoint["目标实例"] = target_number
        checkpoint["恢复耗时"] = seconds
        checkpoint["记录状态"] = "恢复待处理"
        return manifest, "检查点恢复清单已登记，等待启动目标实例"


def transition(restore_lock: str, action: str, reason: str = "") -> tuple[dict[str, Any] | None, str]:
    """按 待处理 → 运行中 → 已恢复 → 已归档 流转清单；失败时清单与路径引用一起退回。"""
    with RESTORE_MUTEX:
        manifest = find_manifest(restore_lock)
        if manifest is None:
            return None, f"恢复清单 {restore_lock} 不存在"
        if action not in ACTION_TARGETS:
            return None, f"动作「{action}」不属于检查点恢复可执行范围"

        current = _text(manifest.get("状态"))
        target = ACTION_TARGETS[action]
        if target not in TRANSITION_RULES.get(current, set()):
            return None, f"恢复清单当前为{current}，不能执行「{action}」"

        if action == "恢复失败":
            return _rollback(manifest, reason or "恢复失败，清单已退回")

        manifest["状态"] = target
        manifest["status"] = target
        manifest["pending"] = target != STATUS_ARCHIVED
        manifest["abnormal"] = False
        manifest["更新时间"] = _now()

        if target == STATUS_RESTORED:
            checkpoint = _find_checkpoint(manifest)
            if checkpoint is not None:
                checkpoint["status"] = "已恢复"
                checkpoint["记录状态"] = "已恢复"
                checkpoint["恢复耗时"] = manifest.get("恢复耗时")
        elif target == STATUS_ARCHIVED:
            manifest["恢复锁状态"] = "已释放"
            manifest["路径引用"] = "已释放"
            manifest["归档时间"] = _now()
            _release_path(_text(manifest.get("恢复锁")))

        _sync_consumption(_text(manifest.get("恢复锁")), target, f"清单状态更新为{target}")
        return manifest, f"恢复清单{action}完成，当前状态：{target}"


def _find_checkpoint(manifest: dict[str, Any]) -> dict[str, Any] | None:
    checkpoint_no = _text(manifest.get("记录编号"))
    for row in store.rows("checkpoint"):
        if _text(row.get("记录编号")) == checkpoint_no:
            return row
    return None


def _rollback(manifest: dict[str, Any], reason: str) -> tuple[dict[str, Any], str]:
    restore_lock = _text(manifest.get("恢复锁"))
    previous_status = _text(manifest.get("状态"))

    manifest["状态"] = STATUS_ROLLED_BACK
    manifest["status"] = STATUS_ROLLED_BACK
    manifest["pending"] = False
    manifest["abnormal"] = True
    manifest["恢复锁状态"] = "已释放"
    manifest["路径引用"] = "已释放"
    manifest["回退原因"] = reason
    manifest["回退时间"] = _now()
    manifest["更新时间"] = _now()

    _release_path(restore_lock)

    target_number = _text(manifest.get("目标实例"))
    if _text(manifest.get("目标实例来源")) == "新建":
        store.replace_rows(
            "jobops",
            [row for row in store.rows("jobops") if _instance_number(row) != target_number],
        )
    elif manifest.get("_目标实例准入前"):
        # 复用实例回退到准入前状态，不能只清掉恢复字段。
        _restore_instance_fields(manifest["_目标实例准入前"])
    else:
        for instance in store.rows("jobops"):
            if _instance_number(instance) != target_number:
                continue
            instance.pop("检查点ID", None)
            instance.pop("保存路径", None)
            instance.pop("恢复请求号", None)
            instance.pop("恢复锁", None)
            instance.pop("superseded_by", None)
            _set_instance_status(instance, "待启动")
            break

    for snapshot in manifest.get("旧实例明细") or []:
        _restore_instance_snapshot(snapshot)

    checkpoint = _find_checkpoint(manifest)
    if checkpoint is not None:
        checkpoint["记录状态"] = "恢复已回退"
        checkpoint.pop("恢复锁", None)
        checkpoint.pop("目标实例", None)
        checkpoint.pop("恢复请求号", None)

    record = {
        "id": _next_id(ROLLBACK_MODULE),
        "恢复锁": restore_lock,
        "恢复请求号": manifest.get("恢复请求号"),
        "记录编号": manifest.get("记录编号"),
        "检查点ID": manifest.get("检查点ID"),
        "保存路径": manifest.get("保存路径"),
        "目标实例": target_number,
        "原状态": previous_status,
        "回退原因": reason,
        "回退时间": _now(),
    }
    store.rows(ROLLBACK_MODULE).append(record)
    _sync_consumption(restore_lock, STATUS_ROLLED_BACK, f"清单已回退：{reason}")
    return manifest, f"恢复清单已回退：{reason}"


def consume_manifest(restore_lock: str) -> tuple[dict[str, Any] | None, str]:
    """作业运维入口同步接收同一份清单，并维护自己的消费处理记录。"""
    with RESTORE_MUTEX:
        manifest = find_manifest(restore_lock)
        if manifest is None:
            return None, f"恢复清单 {restore_lock} 不存在"
        status = _text(manifest.get("状态"))
        if status in TERMINAL_STATUSES:
            return None, f"恢复清单当前为{status}，不能再被作业准备消费"

        existing = find_consumption(restore_lock)
        if existing is not None:
            existing["清单状态"] = status
            return existing, f"恢复清单 {restore_lock} 已接收，返回已有处理记录"

        record = {
            "id": _next_id(CONSUMPTION_MODULE),
            "记录编号": manifest.get("记录编号"),
            "恢复锁": restore_lock,
            "恢复请求号": manifest.get("恢复请求号"),
            "检查点ID": manifest.get("检查点ID"),
            "保存路径": manifest.get("保存路径"),
            "目标实例": manifest.get("目标实例"),
            "恢复耗时": manifest.get("恢复耗时"),
            "清单状态": status,
            "消费状态": "已接收",
            "消费时间": _now(),
            "清单更新时间": manifest.get("更新时间"),
            "处理记录": "作业准备已接收恢复清单",
        }
        store.rows(CONSUMPTION_MODULE).append(record)
        return record, "恢复清单已同步到作业准备，处理记录已登记"


def check_instance_start(instance: dict[str, Any]) -> tuple[bool, str]:
    """启动实例前核对清单与保存路径，确保只有一套已校验的恢复清单生效。"""
    number = _instance_number(instance)
    restore_lock = _text(instance.get("恢复锁"))
    save_path = _instance_path(instance)

    if restore_lock:
        manifest = find_manifest(restore_lock)
        if manifest is None:
            return False, f"实例 {number} 关联的恢复清单 {restore_lock} 不存在，不能启动"
        status = _text(manifest.get("状态"))
        if status in TERMINAL_STATUSES:
            return False, f"实例 {number} 的恢复清单当前为{status}，不能启动"
        if _text(manifest.get("目标实例")) != number:
            return False, f"恢复清单目标为 {manifest.get('目标实例')}，与实例 {number} 不一致"
        if _text(manifest.get("路径引用")) != "已占用":
            return False, f"恢复清单 {restore_lock} 的保存路径引用已释放，不能启动"

    if save_path:
        for other in store.rows("jobops"):
            if _instance_number(other) == number:
                continue
            if _instance_path(other) != save_path:
                continue
            if _text(other.get("status")) not in JOB_ACTIVE_STATUSES:
                continue
            return False, f"保存路径 {save_path} 已由实例 {_instance_number(other)} 持有，不能启动两个实例"
        for holder in _path_rows(save_path):
            if _text(holder.get("恢复锁")) != restore_lock:
                return False, f"保存路径 {save_path} 已被恢复请求 {holder.get('恢复请求号')} 占用"
    return True, ""


def mark_instance_started(instance: dict[str, Any]) -> None:
    """恢复实例真正启动后，把对应清单从待处理推进到运行中。"""
    restore_lock = _text(instance.get("恢复锁"))
    if not restore_lock:
        return
    manifest = find_manifest(restore_lock)
    if manifest is None or _text(manifest.get("状态")) != STATUS_PENDING:
        return
    manifest["状态"] = STATUS_RUNNING
    manifest["status"] = STATUS_RUNNING
    manifest["pending"] = True
    manifest["更新时间"] = _now()
    _sync_consumption(restore_lock, STATUS_RUNNING, "目标实例已启动，清单进入运行中")
