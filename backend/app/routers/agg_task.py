"""聚合任务 时间线接口：作业时间序列、运行生命周期与事件流水。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.lifecycle.ledger import LifecycleError, run_ledger
from app.lifecycle.service import agg_lifecycle
from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.agg_task import AggTaskService

router = APIRouter(prefix="/api/agg_task", tags=["聚合任务"])
service = AggTaskService()
LIST_FIELDS = ["任务编号", "目标指标", "聚合维度", "计算周期", "依赖上游", "并行度", "超时分钟", "重试上限"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    """时间线数据来自事件流水投影，未运行任务不返回编造时间。"""
    items = service.get_timeline()
    return {"items": items}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id", "name", "status", "run_id", "start", "end"]
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=agg_task.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"聚合计算 {entry_id} 不存在")
    return entry


@router.get("/{entry_id}/runs")
def list_runs(entry_id: int) -> dict[str, Any]:
    """返回该任务的全部运行记录与事件流水（状态流水数据）。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"聚合计算 {entry_id} 不存在")
    return agg_lifecycle.task_timeline(entry_id)


@router.get("/runs/{run_id}/events")
def get_run_events(run_id: str) -> dict[str, Any]:
    """按运行编号定位一次计算的全部开始/写入/结束事件。"""
    run = run_ledger.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行 {run_id} 不存在")
    return {"run": run.as_dict(), "events": [e.as_dict() for e in run.events]}


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="聚合计算已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """提交调度 / 重跑失败 / 取消任务，均落到真实生命周期命令。"""
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


# ---------------------------------------------------------------------------
# 生命周期命令接口：每次调用都产生一条可追溯事件
# ---------------------------------------------------------------------------

def _guard(callable_):
    try:
        return callable_()
    except LifecycleError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/{entry_id}/runs/start", response_model=ActionResult)
def start_run(entry_id: int) -> ActionResult:
    """开启一次聚合运行；失败/超时后调用即为关联恢复运行。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"聚合计算 {entry_id} 不存在")
    run = _guard(lambda: agg_lifecycle.start_run(entry_id))
    return ActionResult(
        ok=True,
        message=f"运行 {run.run_id} 已启动"
        + (f"（自 {run.recovered_from} 恢复）" if run.recovered_from else ""),
        entry=run.as_dict(),
    )


@router.post("/{entry_id}/runs/write", response_model=ActionResult)
def write_metric(
    entry_id: int,
    metric: str = Query(..., description="指标名，需在任务口径清单内"),
    value: float = Query(..., description="本次计算得到的指标值"),
) -> ActionResult:
    """记录一次指标写入事件（写入暂存区，未确认前查询口不可见）。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"聚合计算 {entry_id} 不存在")
    run = _guard(lambda: agg_lifecycle.write_metric(entry_id, metric, value))
    return ActionResult(ok=True, message=f"指标 {metric}={value} 写入事件已记录", entry=run.as_dict())


@router.post("/{entry_id}/runs/complete-writes", response_model=ActionResult)
def complete_writes(entry_id: int) -> ActionResult:
    """全部指标写完，进入等待完成回执阶段。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"聚合计算 {entry_id} 不存在")
    run = _guard(lambda: agg_lifecycle.complete_writes(entry_id))
    return ActionResult(ok=True, message="全部指标写入完成，等待完成回执", entry=run.as_dict())


@router.post("/{entry_id}/runs/confirm", response_model=ActionResult)
def confirm_run(entry_id: int) -> ActionResult:
    """完成回执到达：校验指标完整后发布快照（支持迟到回执）。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"聚合计算 {entry_id} 不存在")
    run, published = _guard(lambda: agg_lifecycle.confirm_run(entry_id))
    return ActionResult(
        ok=True,
        message=f"运行 {run.run_id} 已确认，发布 {len(published)} 个指标快照",
        entry={"run": run.as_dict(), "published": published},
    )


@router.post("/{entry_id}/runs/timeout", response_model=ActionResult)
def mark_timeout(entry_id: int) -> ActionResult:
    """完成回执超时：运行转“回执超时”，候选快照不发布。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"聚合计算 {entry_id} 不存在")
    run = _guard(lambda: agg_lifecycle.mark_confirm_timeout(entry_id))
    return ActionResult(ok=True, message="完成回执超时已记录，查询口保持上一已确认版本", entry=run.as_dict())


@router.post("/{entry_id}/runs/fail", response_model=ActionResult)
def fail_run(
    entry_id: int,
    reason: str = Query(..., description="失败原因"),
    partial_metric: str | None = Query(default=None, description="可选：失败前最后写入的半成品指标"),
    partial_value: float | None = Query(default=None),
) -> ActionResult:
    """记录运行失败事件；携带的半成品写入会被拒绝发布。"""
    if service.get_entry(entry_id) is None:
        raise HTTPException(status_code=404, detail=f"聚合计算 {entry_id} 不存在")
    run = _guard(lambda: agg_lifecycle.fail_run(
        entry_id, reason, partial_metric=partial_metric, partial_value=partial_value))
    return ActionResult(ok=True, message=f"运行 {run.run_id} 失败事件已记录", entry=run.as_dict())
