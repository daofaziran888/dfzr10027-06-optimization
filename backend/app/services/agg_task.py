"""聚合任务 时间线服务。

历史问题（已修复）
------------------
旧实现按列表下标 ``today - timedelta(...)`` 推算开始/结束时间，与真实计算无关；
日志甚至在任务计算失败时打印“任务完成”。

现在任务时间线完全由 ``app.lifecycle`` 的事件流水回放得到：
没有运行事件的任务保持“待调度”且不编造时间；运行的开始/结束时间取自真实事件，
日志按事件类型逐条生成，失败运行只可能出现失败结论。
"""
from __future__ import annotations

from typing import Any

from app.lifecycle.catalog import snapshot_catalog
from app.lifecycle.ledger import RECOVERABLE_STATES, RunStatus, run_ledger
from app.lifecycle.service import TASK_MODULE, agg_lifecycle
from app.store import store

REQUIRED_FIELDS = ["任务编号", "目标指标", "聚合维度"]
STATUS_ORDER = ["待调度", "计算中", "已完成", "计算失败", "写入中", "等待确认", "回执超时"]

# 动作映射到真实生命周期命令；不再存在把失败直接改成完成的裸状态跳转。
ACTION_RULES = ["提交调度", "重跑失败", "取消任务"]

_EVENT_LABELS = {
    "RUN_STARTED": "作业启动，运行编号 {run_id}",
    "WRITE_BEGAN": "开始写入指标 {metric}",
    "METRIC_WRITTEN": "指标 {metric} 写入完成，值 = {value}",
    "WRITES_COMPLETED": "全部指标写入完成，等待完成回执",
    "RUN_FINISHED": "完成回执已确认，运行成功结束",
    "RUN_FAILED": "运行失败：{reason}",
    "CONFIRM_TIMEOUT": "完成回执超时，候选结果暂不发布",
}


class AggTaskService:
    # ---- 时间线：全部来自真实流水 ----------------------------------------
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(TASK_MODULE)
        items: list[dict[str, Any]] = []
        for i, row in enumerate(rows):
            task_id = int(row.get("id", i + 1))
            runs = run_ledger.list_runs(task_id)
            events = run_ledger.events(task_id=task_id)
            latest = runs[-1] if runs else None

            if latest is None:
                # 未真正运行过：不再按下标编造时间，时间字段留空。
                items.append({
                    "id": task_id,
                    "name": str(row.get("任务编号", "")),
                    "start": None,
                    "end": None,
                    "status": "待调度",
                    "run_id": None,
                    "logs": "尚未产生运行事件（待调度）",
                    "published_snapshots": [],
                    "pending_candidate": None,
                })
                continue

            items.append({
                "id": task_id,
                "name": str(row.get("任务编号", "")),
                "start": latest.started_at,
                "end": latest.finished_at,
                "status": latest.status.value,
                "run_id": latest.run_id,
                "recovered_from": latest.recovered_from,
                "logs": self._render_log(events),
                "published_snapshots": [
                    snapshot.as_dict() for snapshot in snapshot_catalog.list_published(task_id)
                ],
                "pending_candidate": snapshot_catalog.pending_candidate(task_id),
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(TASK_MODULE, entry_id)
        if entry is None:
            return None
        detail = dict(entry)
        detail["timeline"] = agg_lifecycle.task_timeline(entry_id)
        detail["required_metrics"] = agg_lifecycle.required_metrics(entry_id)
        return detail

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(TASK_MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = "待调度"
        entry["pending"] = True
        entry["abnormal"] = False
        # 口径指标清单：默认单指标 gmv，登记时可用逗号分隔声明多个指标。
        metrics = str(values.get("metrics") or values.get("指标清单") or "gmv").strip()
        entry["metrics"] = [m.strip() for m in metrics.split(",") if m.strip()]
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(TASK_MODULE, entry_id)
        if entry is None:
            return None, f"聚合计算 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于聚合任务可执行范围"

        latest = run_ledger.latest_run(entry_id)
        if action == "提交调度":
            if latest is not None and not latest.is_terminal():
                return None, f"运行 {latest.run_id} 仍在 {latest.status.value}，不能重复提交"
            run = agg_lifecycle.start_run(entry_id)
            return entry, f"聚合计算已提交，开启运行 {run.run_id}"

        if action == "重跑失败":
            if latest is None or latest.status not in RECOVERABLE_STATES:
                return None, "只有计算失败或回执超时的运行才允许从失败状态恢复重跑"
            run = agg_lifecycle.start_run(entry_id)
            return entry, f"已从 {run.recovered_from} 恢复，开启关联运行 {run.run_id}"

        # 取消任务：仅允许对进行中的运行落失败事件，绝不允许把运行改成“已完成”。
        if latest is None or latest.is_terminal() or latest.status == RunStatus.TIMED_OUT:
            return None, "当前没有可取消的进行中运行"
        agg_lifecycle.fail_run(entry_id, reason="人工取消任务")
        return entry, f"运行 {latest.run_id} 已取消并记录失败事件"

    def _render_log(self, events: list) -> str:
        lines: list[str] = []
        for event in events:
            template = _EVENT_LABELS.get(event.event_type, event.event_type)
            payload = event.payload or {}
            lines.append(
                f"[{event.occurred_at}] [{event.event_id}] [{event.run_id}] "
                f"{template.format(run_id=event.run_id, **payload)}"
            )
        return "\n".join(lines)
