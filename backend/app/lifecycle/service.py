"""聚合运行生命周期编排服务。

对外暴露的每个命令都对应流水里的真实事件，时间戳取自可注入时钟，
状态机由 :mod:`app.lifecycle.ledger` 强制约束，发布门禁由
:mod:`app.lifecycle.catalog` 强制约束。
"""
from __future__ import annotations

from typing import Any

from app.lifecycle.catalog import snapshot_catalog
from app.lifecycle.ledger import (
    RECOVERABLE_STATES,
    RUN_TERMINAL_STATES,
    RunRecord,
    RunStatus,
    run_ledger,
)
from app.lifecycle.ledger import LifecycleError  # 再导出，方便上层统一引用
from app.store import store

TASK_MODULE = "agg_task"


class AggLifecycleService:
    # ---- 任务与指标口径 --------------------------------------------------
    def required_metrics(self, task_id: int) -> list[str]:
        """任务要求写齐的指标清单；登记在任务行的 metrics 字段，缺省为单指标 gmv。"""
        entry = store.find(TASK_MODULE, task_id)
        if entry is None:
            raise LifecycleError(f"聚合计算 {task_id} 不存在或已归档")
        raw = entry.get("metrics") or entry.get("指标清单") or "gmv"
        if isinstance(raw, (list, tuple)):
            return [str(m).strip() for m in raw if str(m).strip()]
        return [part.strip() for part in str(raw).split(",") if part.strip()]

    # ---- 运行生命周期命令 -----------------------------------------------
    def start_run(self, task_id: int) -> RunRecord:
        """开启一次新的聚合运行；若上一次运行停在终态（失败/超时），则作为关联恢复运行。"""
        latest = run_ledger.latest_run(task_id)
        if latest is not None and not latest.is_terminal() and latest.status != RunStatus.TIMED_OUT:
            raise LifecycleError(
                f"任务 {task_id} 已有进行中的运行 {latest.run_id}（{latest.status.value}），不能重复启动"
            )
        attempt = (latest.attempt + 1) if latest else 1
        run_id = f"RUN-{task_id:04d}-{attempt:02d}"
        recovered_from = latest.run_id if latest and latest.status in RECOVERABLE_STATES else None
        run_ledger.append(
            event_type="RUN_STARTED",
            task_id=task_id,
            run_id=run_id,
            run_attempt=attempt,
            status=RunStatus.RUNNING,
            payload={"recovered_from": recovered_from},
            recovered_from=recovered_from,
        )
        self._sync_task_status(task_id)
        record = run_ledger.get_run(run_id)
        assert record is not None
        return record

    def write_metric(self, task_id: int, metric: str, value: Any) -> RunRecord:
        """记录一次指标写入；只接受当前进行中运行，写入即进暂存区但查询口不可见。"""
        run = self._require_active_run(task_id)
        if metric not in self.required_metrics(task_id):
            raise LifecycleError(f"指标 {metric} 不在任务 {task_id} 的口径清单内，拒绝写入")
        if run.status == RunStatus.RUNNING:
            run_ledger.append(
                event_type="WRITE_BEGAN",
                task_id=task_id,
                run_id=run.run_id,
                run_attempt=run.attempt,
                status=RunStatus.WRITING,
                payload={"metric": metric},
            )
        event = run_ledger.append(
            event_type="METRIC_WRITTEN",
            task_id=task_id,
            run_id=run.run_id,
            run_attempt=run.attempt,
            status=RunStatus.WRITING,
            payload={"metric": metric, "value": value},
        )
        record = run_ledger.get_run(run.run_id)
        assert record is not None
        snapshot_catalog.register_write(
            run=record,
            metric=metric,
            value=value,
            write_event_id=event.event_id,
            required_metrics=self.required_metrics(task_id),
        )
        return record

    def complete_writes(self, task_id: int) -> RunRecord:
        """全部指标写完，运行进入“等待完成回执”。指标没写齐则不允许进入该阶段。"""
        run = self._require_active_run(task_id)
        required = self.required_metrics(task_id)
        missing = [m for m in required if m not in run.metrics_written]
        if missing:
            raise LifecycleError(
                f"运行 {run.run_id} 还有指标未写入（{'、'.join(missing)}），不能结束写入阶段"
            )
        run_ledger.append(
            event_type="WRITES_COMPLETED",
            task_id=task_id,
            run_id=run.run_id,
            run_attempt=run.attempt,
            status=RunStatus.AWAITING_CONFIRM,
            payload={"metrics": list(run.metrics_written)},
        )
        record = run_ledger.get_run(run.run_id)
        assert record is not None
        snapshot_catalog.mark_awaiting_confirm(record, required)
        self._sync_task_status(task_id)
        return record

    def confirm_run(self, task_id: int) -> tuple[RunRecord, list[dict[str, Any]]]:
        """完成回执到达：校验完整后发布快照。迟到回执（超时后到达）同样在此确认。"""
        run = run_ledger.latest_run(task_id)
        if run is None or run.is_terminal():
            raise LifecycleError(f"任务 {task_id} 没有可确认的运行")
        if run.status not in (RunStatus.AWAITING_CONFIRM, RunStatus.TIMED_OUT):
            raise LifecycleError(
                f"运行 {run.run_id} 当前状态 {run.status.value}，没有待确认的完成回执"
            )
        event = run_ledger.append(
            event_type="RUN_FINISHED",
            task_id=task_id,
            run_id=run.run_id,
            run_attempt=run.attempt,
            status=RunStatus.SUCCEEDED,
            payload={"metrics": sorted(run.metrics_written)},
        )
        record = run_ledger.get_run(run.run_id)
        assert record is not None
        published = snapshot_catalog.publish_run(
            record, self.required_metrics(task_id), event.event_id
        )
        self._sync_task_status(task_id)
        return record, published

    def mark_confirm_timeout(self, task_id: int) -> RunRecord:
        """完成回执超时：流水留痕，运行转“回执超时”，已写结果保持候选、不发布。"""
        run = self._require_active_run(task_id)
        if run.status != RunStatus.AWAITING_CONFIRM:
            raise LifecycleError(
                f"运行 {run.run_id} 当前状态 {run.status.value}，不满足回执超时判定条件"
            )
        run_ledger.append(
            event_type="CONFIRM_TIMEOUT",
            task_id=task_id,
            run_id=run.run_id,
            run_attempt=run.attempt,
            status=RunStatus.TIMED_OUT,
            payload={"reason": "完成回执在超时窗口内未到达"},
        )
        record = run_ledger.get_run(run.run_id)
        assert record is not None
        snapshot_catalog.mark_timeout(record)
        self._sync_task_status(task_id)
        return record

    def fail_run(self, task_id: int, reason: str, *, partial_metric: str | None = None,
                 partial_value: Any = None) -> RunRecord:
        """运行失败：可携带最后一个半成品写入；失败事件落流水，半成品被拒绝发布。"""
        run = self._require_active_run(task_id)
        if partial_metric is not None and partial_metric in self.required_metrics(task_id):
            if run.status == RunStatus.RUNNING:
                run_ledger.append(
                    event_type="WRITE_BEGAN",
                    task_id=task_id,
                    run_id=run.run_id,
                    run_attempt=run.attempt,
                    status=RunStatus.WRITING,
                    payload={"metric": partial_metric},
                )
            event = run_ledger.append(
                event_type="METRIC_WRITTEN",
                task_id=task_id,
                run_id=run.run_id,
                run_attempt=run.attempt,
                status=RunStatus.WRITING,
                payload={"metric": partial_metric, "value": partial_value},
            )
            record = run_ledger.get_run(run.run_id)
            assert record is not None
            snapshot_catalog.register_write(
                run=record,
                metric=partial_metric,
                value=partial_value,
                write_event_id=event.event_id,
                required_metrics=self.required_metrics(task_id),
            )
        run_ledger.append(
            event_type="RUN_FAILED",
            task_id=task_id,
            run_id=run.run_id,
            run_attempt=run.attempt,
            status=RunStatus.FAILED,
            payload={"reason": reason},
        )
        record = run_ledger.get_run(run.run_id)
        assert record is not None
        snapshot_catalog.reject_run(record, reason)
        self._sync_task_status(task_id)
        return record

    # ---- 查询辅助 -------------------------------------------------------
    def task_timeline(self, task_id: int) -> dict[str, Any]:
        """汇总任务的运行历史、当前状态与流水，供时间线页面使用。"""
        runs = run_ledger.list_runs(task_id)
        latest = runs[-1] if runs else None
        return {
            "task_id": task_id,
            "current_status": latest.status.value if latest else "待调度",
            "latest_run_id": latest.run_id if latest else None,
            "run_count": len(runs),
            "runs": [run.as_dict() for run in runs],
            "events": [event.as_dict() for event in run_ledger.events(task_id=task_id)],
            "pending_candidate": snapshot_catalog.pending_candidate(task_id),
        }

    def _require_active_run(self, task_id: int) -> RunRecord:
        run = run_ledger.latest_run(task_id)
        if run is None:
            raise LifecycleError(f"任务 {task_id} 还没有启动过运行，请先开启运行")
        if run.is_terminal():
            raise LifecycleError(
                f"运行 {run.run_id} 已结束（{run.status.value}），请从失败状态恢复后开启新运行"
            )
        if run.status == RunStatus.TIMED_OUT:
            raise LifecycleError(
                f"运行 {run.run_id} 回执超时：仅允许迟到回执确认，或从该状态恢复重跑"
            )
        return run

    def _sync_task_status(self, task_id: int) -> None:
        """把任务行状态同步为最新运行的真实状态，状态只可能来自流水投影。"""
        entry = store.find(TASK_MODULE, task_id)
        if entry is None:
            return
        latest = run_ledger.latest_run(task_id)
        if latest is None:
            return
        entry["status"] = latest.status.value
        entry["latest_run_id"] = latest.run_id
        entry["abnormal"] = latest.status in (RunStatus.FAILED, RunStatus.TIMED_OUT)
        entry["pending"] = not latest.is_terminal()


agg_lifecycle = AggLifecycleService()
