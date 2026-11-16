"""流作业运维接口：维护作业实例，覆盖恢复清单接收、启动作业、暂停作业、停止作业等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import restore
from app.services.jobops import JobopsService

router = APIRouter(prefix="/api/jobops", tags=["流作业运维"])

service = JobopsService()

LIST_FIELDS = [
    "实例编号",
    "作业名称",
    "运行节点",
    "启动时间",
    "运行时长",
    "异常次数",
    "重启次数",
    "实例状态",
    "检查点ID",
    "保存路径",
    "恢复锁",
]
STATUSES = ["待启动", "运行中", "已暂停", "已停止"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按实例编号、检查点ID、恢复锁检索"),
    status: str | None = Query(default=None, description="待启动、运行中、已暂停、已停止"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按实例编号与状态过滤流作业运维列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出流作业运维清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "jobops", "total": total, "items": items}


@router.get("/manifests", response_model=PageResult[dict])
def list_manifests(
    keyword: str | None = Query(default=None, description="按恢复请求号、目标实例、恢复锁检索"),
    status: str | None = Query(default=None, description="待处理、运行中、已恢复、已归档、已回退"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """作业准备入口读取同一份检查点恢复数据清单，不另存一套。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = restore.list_manifests(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/manifests/{restore_lock}/consume", response_model=ActionResult)
def consume_manifest(restore_lock: str) -> ActionResult:
    """同步接收一份恢复清单；重复接收返回同一条消费处理记录。"""
    record, message = service.consume_manifest(restore_lock)
    if record is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=record)


@router.get("/consumptions/{restore_lock}", response_model=dict)
def get_consumption(restore_lock: str) -> dict:
    """读取作业准备侧自己的消费处理记录。"""
    record = service.find_consumption(restore_lock)
    if record is None:
        raise HTTPException(status_code=404, detail=f"恢复清单 {restore_lock} 尚无消费处理记录")
    return record


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条作业实例明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"作业实例 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条作业实例，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="作业实例已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条作业实例执行启动作业、暂停作业、停止作业；恢复实例启动前必须核对清单与保存路径。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
