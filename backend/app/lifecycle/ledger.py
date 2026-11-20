"""聚合运行事件流水（Event Log）。

设计要点
--------
1. 事件只追加、不可改写：每次状态变化都产生一条带真实时间戳的事件，
   任务时间线由事件回放得到，而不是按列表位置推算。
2. 每条事件都带 run_id：一次聚合计算的 RUN_STARTED / METRIC_WRITTEN /
   RUN_FINISHED / RUN_FAILED 等记录全部归属同一次运行，计算与结果可对账。
3. 从失败状态恢复（重试）会新开一次 run_id，并通过 recovered_from 关联上一次失败运行。
4. 运行状态由事件序列回放重建，非法流转直接拒绝，因此“计算失败的任务被标成完成”
   这类只改展示状态、没有真实完成事件的情况无法发生。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from itertools import count
from typing import Any

from app.store import store

EVENT_TABLE = "_agg_run_events"


class RunStatus(str, Enum):
    """聚合运行的真实生命周期状态。"""

    PENDING = "待启动"
    RUNNING = "计算中"
    WRITING = "写入中"
    AWAITING_CONFIRM = "等待确认"
    SUCCEEDED = "已完成"
    FAILED = "计算失败"
    TIMED_OUT = "回执超时"


# 已关闭的终态：成功或失败。回执超时不算终态——它仍可能收到迟到回执。
RUN_TERMINAL_STATES = {RunStatus.SUCCEEDED, RunStatus.FAILED}

# 可被恢复（重跑）的状态：失败与回执超时。
RECOVERABLE_STATES = {RunStatus.FAILED, RunStatus.TIMED_OUT}

# 允许的状态流转；任何不在表里的跳转都会被拒绝。
_ALLOWED_TRANSITIONS: dict[RunStatus, set[RunStatus]] = {
    RunStatus.PENDING: {RunStatus.RUNNING},
    RunStatus.RUNNING: {RunStatus.WRITING, RunStatus.FAILED},
    RunStatus.WRITING: {RunStatus.AWAITING_CONFIRM, RunStatus.FAILED},
    # 完成回执迟到：先由超时看护判定为“回执超时”，回执到达后仍可确认为“已完成”。
    RunStatus.AWAITING_CONFIRM: {RunStatus.SUCCEEDED, RunStatus.TIMED_OUT, RunStatus.FAILED},
    RunStatus.TIMED_OUT: {RunStatus.SUCCEEDED},
    RunStatus.SUCCEEDED: set(),
    RunStatus.FAILED: set(),
}


class Clock:
    """可注入时钟：仿真时用固定序列时间，线上用真实墙钟时间。"""

    def __init__(self) -> None:
        self._fixed: datetime | None = None

    def now(self) -> datetime:
        return self._fixed or datetime.now()

    def pin(self, moment: datetime) -> None:
        self._fixed = moment

    def resume(self) -> None:
        self._fixed = None


clock = Clock()


@dataclass
class AggEvent:
    """流水里的一条不可变事件。"""

    seq: int
    event_id: str
    event_type: str
    task_id: int
    run_id: str
    run_attempt: int
    occurred_at: str
    run_status: str
    payload: dict[str, Any] = field(default_factory=dict)
    recovered_from: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "task_id": self.task_id,
            "run_id": self.run_id,
            "run_attempt": self.run_attempt,
            "occurred_at": self.occurred_at,
            "run_status": self.run_status,
            "payload": dict(self.payload),
            "recovered_from": self.recovered_from,
        }


@dataclass
class RunRecord:
    """一次聚合运行的投影状态，由事件回放得到。"""

    run_id: str
    task_id: int
    attempt: int
    status: RunStatus
    recovered_from: str | None
    started_at: str | None = None
    last_event_at: str | None = None
    finished_at: str | None = None
    metrics_written: set[str] = field(default_factory=set)
    metric_values: dict[str, Any] = field(default_factory=dict)
    written_event_ids: list[str] = field(default_factory=list)
    end_event_id: str | None = None
    failure_reason: str | None = None
    events: list[AggEvent] = field(default_factory=list)

    def is_terminal(self) -> bool:
        return self.status in RUN_TERMINAL_STATES

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "task_id": self.task_id,
            "attempt": self.attempt,
            "status": self.status.value,
            "recovered_from": self.recovered_from,
            "started_at": self.started_at,
            "last_event_at": self.last_event_at,
            "finished_at": self.finished_at,
            "metrics_written": sorted(self.metrics_written),
            "metric_values": dict(self.metric_values),
            "written_event_ids": list(self.written_event_ids),
            "end_event_id": self.end_event_id,
            "failure_reason": self.failure_reason,
            "event_ids": [e.event_id for e in self.events],
        }


class LifecycleError(RuntimeError):
    """生命周期非法流转或数据不完整时抛出。"""


class RunLedger:
    """在 store 的 ``_agg_run_events`` 表上维护只追加事件流与运行投影。"""

    def __init__(self) -> None:
        self._seq = count(1)

    # ---- 底层追加 -------------------------------------------------------
    def append(
        self,
        *,
        event_type: str,
        task_id: int,
        run_id: str,
        run_attempt: int,
        status: RunStatus,
        payload: dict[str, Any] | None = None,
        recovered_from: str | None = None,
    ) -> AggEvent:
        table = store.rows(EVENT_TABLE)
        seq = max((int(row.get("seq", 0)) for row in table), default=0) + 1
        event = AggEvent(
            seq=seq,
            event_id=f"EVT-{seq:06d}",
            event_type=event_type,
            task_id=task_id,
            run_id=run_id,
            run_attempt=run_attempt,
            occurred_at=clock.now().isoformat(timespec="seconds"),
            run_status=status.value,
            payload=dict(payload or {}),
            recovered_from=recovered_from,
        )
        table.append(event.as_dict())
        return event

    # ---- 回放查询 -------------------------------------------------------
    def events(self, *, task_id: int | None = None, run_id: str | None = None) -> list[AggEvent]:
        rows = list(store.rows(EVENT_TABLE))
        if task_id is not None:
            rows = [r for r in rows if int(r["task_id"]) == task_id]
        if run_id is not None:
            rows = [r for r in rows if r["run_id"] == run_id]
        return [self._to_event(r) for r in sorted(rows, key=lambda r: int(r["seq"]))]

    def get_run(self, run_id: str) -> RunRecord | None:
        runs = self._replay(run_id=run_id)
        return runs.get(run_id)

    def list_runs(self, task_id: int | None = None) -> list[RunRecord]:
        runs = self._replay(task_id=task_id)
        return sorted(runs.values(), key=lambda r: (r.task_id, r.attempt))

    def latest_run(self, task_id: int) -> RunRecord | None:
        runs = self.list_runs(task_id)
        return runs[-1] if runs else None

    def _replay(
        self, *, task_id: int | None = None, run_id: str | None = None
    ) -> dict[str, RunRecord]:
        records: dict[str, RunRecord] = {}
        for event in self.events(task_id=task_id, run_id=run_id):
            record = records.get(event.run_id)
            if record is None:
                record = RunRecord(
                    run_id=event.run_id,
                    task_id=event.task_id,
                    attempt=event.run_attempt,
                    status=RunStatus.PENDING,
                    recovered_from=event.recovered_from,
                )
                records[event.run_id] = record
            self._apply(record, event)
            record.events.append(event)
        return records

    def _apply(self, record: RunRecord, event: AggEvent) -> None:
        new_status = RunStatus(event.run_status)
        if new_status != record.status and new_status not in _ALLOWED_TRANSITIONS[record.status]:
            raise LifecycleError(
                f"运行 {record.run_id} 非法状态流转：{record.status.value} -> {new_status.value}"
            )
        record.status = new_status
        record.last_event_at = event.occurred_at
        if event.event_type == "RUN_STARTED":
            record.started_at = event.occurred_at
        elif event.event_type == "METRIC_WRITTEN":
            metric = str(event.payload.get("metric"))
            record.metrics_written.add(metric)
            record.metric_values[metric] = event.payload.get("value")
            record.written_event_ids.append(event.event_id)
        elif event.event_type in ("RUN_FINISHED", "RUN_FAILED", "CONFIRM_TIMEOUT"):
            record.finished_at = event.occurred_at
            record.end_event_id = event.event_id
            if event.event_type == "RUN_FAILED":
                record.failure_reason = str(event.payload.get("reason", ""))
            elif event.event_type == "CONFIRM_TIMEOUT":
                record.failure_reason = "完成回执超时"

    @staticmethod
    def _to_event(row: dict[str, Any]) -> AggEvent:
        return AggEvent(
            seq=int(row["seq"]),
            event_id=str(row["event_id"]),
            event_type=str(row["event_type"]),
            task_id=int(row["task_id"]),
            run_id=str(row["run_id"]),
            run_attempt=int(row["run_attempt"]),
            occurred_at=str(row["occurred_at"]),
            run_status=str(row["run_status"]),
            payload=dict(row.get("payload") or {}),
            recovered_from=row.get("recovered_from"),
        )


run_ledger = RunLedger()
