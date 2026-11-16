"""检查点恢复业务规则：状态流转、恢复准入、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.services import restore
from app.services.locks import RESTORE_MUTEX
from app.store import store

MODULE = "checkpoint"
REQUIRED_FIELDS = ["记录编号", "流作业名", "检查点ID"]
STATUS_ORDER = ["已创建", "已恢复", "已过期", "已清理"]
ACTION_TARGETS = {"恢复状态": "已创建", "清理记录": "已清理", "归档记录": "已过期"}
ACTION_SOURCES = {
    "恢复状态": {"已创建"},
    "清理记录": {"已恢复"},
    "归档记录": {"已创建", "已恢复", "已过期"},
}
NEGATIVE_ACTIONS = []
RESTORE_FIELDS = ["检查点ID", "保存路径", "状态大小"]


class CheckpointService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            fields = ("记录编号", "流作业名", "检查点ID", "保存路径", "目标实例", "恢复锁")
            rows = [row for row in rows if any(keyword in str(row.get(field, "")) for field in fields)]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        for field in ("状态大小", "保存路径", "创建时间", "恢复耗时", "记录状态"):
            if values.get(field) not in (None, ""):
                entry[field] = values.get(field)
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(
        self,
        entry_id: int,
        action: str,
        values: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        values = values or {}
        with RESTORE_MUTEX:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"检查点记录 {entry_id} 不存在或已归档"
            if action not in ACTION_TARGETS:
                return None, f"动作「{action}」不属于检查点恢复可执行范围"

            current = str(entry.get("status") or "")
            if current not in ACTION_SOURCES[action]:
                hints = {
                    "恢复状态": "只有已创建的检查点可以发起恢复；已恢复的记录需先归档再重新登记",
                    "清理记录": "只有已恢复的检查点可以清理",
                    "归档记录": "只有已创建、已恢复或已过期的检查点可以归档",
                }
                return None, f"检查点记录当前为{current}，不能执行「{action}」：{hints[action]}"

            if action == "恢复状态":
                # 恢复动作不再直接改状态，而是走统一的恢复准入与恢复清单。
                manifest, message = restore.admit_restore(entry_id, values)
                if manifest is None:
                    return None, message
                entry["abnormal"] = False
                return {"checkpoint": entry, "manifest": restore.public_view(manifest)}, message

            target = ACTION_TARGETS[action]
            if target not in STATUS_ORDER:
                return None, f"目标状态「{target}」不在允许的状态序列里"
            if action == "归档记录" and entry.get("恢复锁"):
                active = restore.find_manifest(str(entry.get("恢复锁")))
                if active is not None and str(active.get("状态")) in restore.ACTIVE_STATUSES:
                    return None, f"检查点仍持有恢复清单 {active.get('恢复锁')}，请先归档恢复清单"
            entry["status"] = target
            entry["pending"] = target != STATUS_ORDER[-1]
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            entry["记录状态"] = target
            return entry, f"检查点记录已{action}"

    def restore_conditions(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """只读核对恢复条件，供页面在发起恢复前预览。"""
        with RESTORE_MUTEX:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"检查点记录 {entry_id} 不存在或已归档"
            missing = [field for field in RESTORE_FIELDS if not str(entry.get(field) or "").strip()]
            blockers: list[str] = []
            if missing:
                blockers.append(f"缺少恢复字段：{'、'.join(missing)}")
            if str(entry.get("status") or "") in {"已过期", "已清理"}:
                blockers.append(f"记录状态为{entry.get('status')}，不满足恢复条件")
            active = [
                row for row in store.rows(restore.MANIFEST_MODULE)
                if str(row.get("记录编号")) == str(entry.get("记录编号"))
                and str(row.get("状态")) in restore.ACTIVE_STATUSES
            ]
            if active:
                blockers.append(f"检查点已被恢复请求 {active[0].get('恢复请求号')} 锁定")
            return {
                "记录编号": entry.get("记录编号"),
                "检查点ID": entry.get("检查点ID"),
                "保存路径": entry.get("保存路径"),
                "状态大小": entry.get("状态大小"),
                "恢复耗时": entry.get("恢复耗时"),
                "可恢复": not blockers,
                "阻塞原因": blockers,
            }, "恢复条件已核对" if not blockers else "；".join(blockers)

    def list_manifests(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        return restore.list_manifests(keyword=keyword, status=status, page=page, size=size)

    def get_manifest(self, restore_lock: str) -> dict[str, Any] | None:
        manifest = restore.find_manifest(restore_lock)
        return restore.public_view(manifest) if manifest is not None else None

    def run_manifest_action(
        self,
        restore_lock: str,
        action: str,
        reason: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        manifest, message = restore.transition(restore_lock, action, reason)
        return (restore.public_view(manifest) if manifest is not None else None), message

    def list_rollbacks(self, *, page: int = 1, size: int = 50) -> tuple[list[dict[str, Any]], int]:
        return restore.list_rollbacks(page=page, size=size)
