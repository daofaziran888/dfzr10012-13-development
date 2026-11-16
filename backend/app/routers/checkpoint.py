"""检查点恢复接口：维护检查点记录，覆盖恢复准入、恢复清单、清理记录、归档记录等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.checkpoint import CheckpointService

router = APIRouter(prefix="/api/checkpoint", tags=["检查点恢复"])

service = CheckpointService()

LIST_FIELDS = [
    "记录编号",
    "流作业名",
    "检查点ID",
    "状态大小",
    "保存路径",
    "创建时间",
    "恢复耗时",
    "记录状态",
    "目标实例",
    "恢复锁",
]
STATUSES = ["已创建", "已恢复", "已过期", "已清理"]
MANIFEST_FIELDS = [
    "恢复请求号",
    "记录编号",
    "检查点ID",
    "目标实例",
    "保存路径",
    "状态大小",
    "恢复耗时",
    "状态",
    "恢复锁",
    "恢复锁状态",
    "路径引用",
]
MANIFEST_STATUSES = ["待处理", "运行中", "已恢复", "已归档", "已回退"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按记录编号、检查点ID、保存路径检索"),
    status: str | None = Query(default=None, description="已创建、已恢复、已过期、已清理"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按记录编号与状态过滤检查点恢复列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出检查点恢复清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "checkpoint", "total": total, "items": items}


@router.get("/manifests", response_model=PageResult[dict])
def list_manifests(
    keyword: str | None = Query(default=None, description="按恢复请求号、检查点ID、目标实例、恢复锁检索"),
    status: str | None = Query(default=None, description="待处理、运行中、已恢复、已归档、已回退"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """读取检查点恢复数据清单：目标实例、恢复耗时与唯一恢复锁都在这里。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_manifests(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/manifests/{restore_lock}", response_model=dict)
def get_manifest(restore_lock: str) -> dict:
    """按恢复锁读取一份恢复清单；不存在时给出可读的错误说明。"""
    manifest = service.get_manifest(restore_lock)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"恢复清单 {restore_lock} 不存在")
    return manifest


@router.post("/manifests/{restore_lock}/actions", response_model=ActionResult)
def run_manifest_action(restore_lock: str, payload: EntryPayload) -> ActionResult:
    """推进恢复清单：开始恢复、完成恢复、归档恢复；恢复失败时清单与路径引用一起退回。"""
    action = str(payload.values.get("action") or "").strip()
    reason = str(payload.values.get("reason") or payload.remark or "").strip()
    manifest, message = service.run_manifest_action(restore_lock, action, reason)
    if manifest is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=manifest)


@router.get("/rollbacks", response_model=PageResult[dict])
def list_rollbacks(page: int = 1, size: int = 50) -> PageResult[dict]:
    """读取恢复回退记录，核对失败时清单与保存路径引用是否一起退回。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_rollbacks(page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}/restore-check", response_model=ActionResult)
def check_restore(entry_id: int) -> ActionResult:
    """只读核对恢复条件：检查点ID、保存路径、状态大小与当前锁占用情况。"""
    result, message = service.restore_conditions(entry_id)
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=bool(result["可恢复"]), message=message, entry=result)


@router.post("/{entry_id}/restore", response_model=ActionResult)
def restore_entry(entry_id: int, payload: EntryPayload) -> ActionResult:
    """从指定检查点发起恢复：校验条件后写入唯一一份恢复数据清单。

    重复提交同一恢复请求号时返回同一份清单与同一个目标实例，不会新建第二个实例。
    """
    entry, message = service.run_action(entry_id, "恢复状态", payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条检查点记录明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"检查点记录 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条检查点记录，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="检查点记录已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条检查点记录执行恢复状态、清理记录、归档记录；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
