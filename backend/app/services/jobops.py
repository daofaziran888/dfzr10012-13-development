"""流作业运维业务规则：状态流转、恢复实例启动准入与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.services import restore
from app.services.locks import RESTORE_MUTEX
from app.store import store

MODULE = "jobops"
REQUIRED_FIELDS = ["实例编号", "作业名称", "运行节点"]
STATUS_ORDER = ["待启动", "运行中", "已暂停", "已停止"]
ACTION_RULES = {"启动作业": "运行中", "暂停作业": "已暂停", "停止作业": "已停止"}
ACTION_SOURCES = {
    "启动作业": {"待启动", "已停止"},
    "暂停作业": {"运行中"},
    "停止作业": {"运行中", "已暂停"},
}
NEGATIVE_ACTIONS = []


class JobopsService:
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
            fields = ("实例编号", "作业名称", "检查点ID", "保存路径", "恢复锁")
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
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        if values.get("恢复锁"):
            entry["恢复锁"] = values.get("恢复锁")
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        with RESTORE_MUTEX:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"作业实例 {entry_id} 不存在或已归档"
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于流作业运维可执行范围"
            target = ACTION_RULES[action]
            if target not in STATUS_ORDER:
                return None, f"目标状态「{target}」不在允许的状态序列里"
            current = str(entry.get("status") or "")
            if current not in ACTION_SOURCES[action]:
                return None, f"作业实例当前为{current}，不能执行「{action}」"
            if action == "启动作业":
                allowed, message = restore.check_instance_start(entry)
                if not allowed:
                    return None, message
            entry["status"] = target
            entry["实例状态"] = target
            # 只有「待启动」算待处理；运行中与已暂停都是已受理后的状态。
            entry["pending"] = target == "待启动"
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            if action == "启动作业":
                restore.mark_instance_started(entry)
            return entry, f"作业实例已{action}"

    def consume_manifest(self, restore_lock: str) -> tuple[dict[str, Any] | None, str]:
        """作业准备入口同步接收检查点恢复数据清单，消费侧维护自己的处理记录。"""
        record, message = restore.consume_manifest(restore_lock)
        return (restore.public_view(record) if record is not None else None), message

    def find_consumption(self, restore_lock: str) -> dict[str, Any] | None:
        return restore.find_consumption(restore_lock)
