"""已确认指标快照目录（读模型）。

发布门禁
--------
查询口只能读到同时满足以下条件的指标快照：

1. 该次运行写齐了任务要求的全部指标（缺写入 / 只写了部分指标一律不发布，
   半成品只保留在暂存区作为流水证据）；
2. 存在真实的 RUN_FINISHED 完成回执事件（“结果写完但回执迟到”时，
   查询口继续返回上一个已确认版本，新结果只作为等待确认的候选版本展示）。

每条已发布快照都带上 run_id 与开始 / 写入 / 结束事件编号，数据服务查询时
逐条回到事件流水核对，确保“查到的值”能和“哪次运行”严格对上。
"""
from __future__ import annotations

from typing import Any

from app.lifecycle.ledger import RunRecord, RunStatus, run_ledger
from app.store import store

PUBLISHED_TABLE = "_agg_metric_snapshots"
STAGING_TABLE = "_agg_snapshot_staging"


class MetricSnapshot:
    """一条已发布指标快照的投影。"""

    def __init__(self, row: dict[str, Any]) -> None:
        self.row = row

    @property
    def key(self) -> tuple[int, str]:
        return int(self.row["task_id"]), str(self.row["metric"])

    def as_dict(self) -> dict[str, Any]:
        return dict(self.row)


class SnapshotCatalog:
    """管理暂存候选、已发布快照与发布门禁。"""

    # ---- 暂存区：每次写入都登记，但暂存数据查询口不可见 ------------------
    def register_write(
        self,
        *,
        run: RunRecord,
        metric: str,
        value: Any,
        write_event_id: str,
        required_metrics: list[str],
    ) -> dict[str, Any]:
        table = store.rows(STAGING_TABLE)
        staged = self._find_staging(run.run_id, metric)
        if staged is None:
            staged = {
                "task_id": run.task_id,
                "run_id": run.run_id,
                "attempt": run.attempt,
                "metric": metric,
                "value": value,
                "write_event_id": write_event_id,
                "required_metrics": list(required_metrics),
                "state": "writing",
            }
            table.append(staged)
        else:
            staged["value"] = value
            staged["write_event_id"] = write_event_id
        return staged

    def mark_awaiting_confirm(self, run: RunRecord, required_metrics: list[str]) -> None:
        """全部指标写完、进入等待完成回执阶段。"""
        for staged in self._staging_of_run(run.run_id):
            staged["state"] = "awaiting_confirm"

    def mark_timeout(self, run: RunRecord) -> None:
        """回执超时：候选保留但不发布，查询口继续读旧版本。"""
        for staged in self._staging_of_run(run.run_id):
            staged["state"] = "confirm_timeout"

    def reject_run(self, run: RunRecord, reason: str) -> None:
        """运行失败：暂存的半成品标记丢弃，永远不进入发布表。"""
        for staged in self._staging_of_run(run.run_id):
            staged["state"] = "rejected"
            staged["reject_reason"] = reason

    # ---- 发布门禁 -------------------------------------------------------
    def publish_run(
        self, run: RunRecord, required_metrics: list[str], confirm_event_id: str
    ) -> list[dict[str, Any]]:
        """运行完成回执到达后，把写齐指标的候选快照发布为新版本。

        缺少任一必写指标时抛 LifecycleError——部分写入的数据不允许发布。
        """
        from app.lifecycle.ledger import LifecycleError

        missing = [m for m in required_metrics if m not in run.metrics_written]
        if missing:
            raise LifecycleError(
                f"运行 {run.run_id} 指标不完整，拒绝发布，缺失：{'、'.join(missing)}"
            )
        if run.status not in (RunStatus.SUCCEEDED, RunStatus.AWAITING_CONFIRM):
            raise LifecycleError(f"运行 {run.run_id} 状态为 {run.status.value}，不允许发布快照")

        published: list[dict[str, Any]] = []
        start_event_id = run.events[0].event_id if run.events else None
        for metric in required_metrics:
            staged = self._find_staging(run.run_id, metric)
            if staged is None:  # 理论上被前面的 missing 检查拦截，双保险
                raise LifecycleError(f"运行 {run.run_id} 缺少指标 {metric} 的写入证据")
            previous = self.get_published(run.task_id, metric)
            version = int(previous.row["version"]) + 1 if previous else 1
            if previous is not None:
                previous.row["active"] = False
                previous.row["superseded_by"] = f"v{version}"
            snapshot = {
                "task_id": run.task_id,
                "metric": metric,
                "value": staged["value"],
                "version": version,
                "version_label": f"v{version}",
                "active": True,
                "run_id": run.run_id,
                "attempt": run.attempt,
                "recovered_from": run.recovered_from,
                "start_event_id": start_event_id,
                "write_event_id": staged["write_event_id"],
                "confirm_event_id": confirm_event_id,
                "event_chain": [start_event_id, staged["write_event_id"], confirm_event_id],
                "published_at": run.finished_at,
                "previous_version": previous.row["version_label"] if previous else None,
            }
            store.rows(PUBLISHED_TABLE).append(snapshot)
            staged["state"] = "published"
            staged["published_version"] = snapshot["version_label"]
            published.append(snapshot)
        return published

    # ---- 查询口 ---------------------------------------------------------
    def get_published(self, task_id: int, metric: str) -> MetricSnapshot | None:
        rows = [
            row
            for row in store.rows(PUBLISHED_TABLE)
            if int(row["task_id"]) == task_id
            and str(row["metric"]) == metric
            and bool(row.get("active"))
        ]
        if not rows:
            return None
        return MetricSnapshot(max(rows, key=lambda r: int(r["version"])))

    def list_published(self, task_id: int) -> list[MetricSnapshot]:
        active = [
            MetricSnapshot(row)
            for row in store.rows(PUBLISHED_TABLE)
            if int(row["task_id"]) == task_id and bool(row.get("active"))
        ]
        return sorted(active, key=lambda s: int(s.row["version"]))

    def pending_candidate(self, task_id: int) -> dict[str, Any] | None:
        """返回正在等待确认（或已超时）的候选版本信息；没有则 None。"""
        latest = run_ledger.latest_run(task_id)
        if latest is None or latest.status not in (
            RunStatus.AWAITING_CONFIRM,
            RunStatus.TIMED_OUT,
        ):
            return None
        staged = [
            dict(row)
            for row in self._staging_of_run(latest.run_id)
            if row["state"] in ("awaiting_confirm", "confirm_timeout")
        ]
        if not staged:
            return None
        return {
            "run_id": latest.run_id,
            "attempt": latest.attempt,
            "state": latest.status.value,
            "since": latest.last_event_at,
            "metrics": {row["metric"]: row["value"] for row in staged},
            "write_event_ids": [row["write_event_id"] for row in staged],
            "note": "结果已完整写入，仍在等待完成回执；查询口暂不发布该版本"
            if latest.status == RunStatus.AWAITING_CONFIRM
            else "完成回执超时，候选版本暂不发布；迟到回执到达后可自动确认",
        }

    # ---- 流水证据核对 ----------------------------------------------------
    def verify_evidence(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        """按快照记录的事件编号回到流水逐条核对，返回核对结果。"""
        checks: dict[str, Any] = {}
        run = run_ledger.get_run(str(snapshot["run_id"]))
        checks["run_exists"] = run is not None
        if run is None:
            return {"verified": False, "checks": checks}
        event_ids = {event.event_id for event in run.events}
        for label in ("start_event_id", "write_event_id", "confirm_event_id"):
            expected = snapshot.get(label)
            checks[label] = {
                "event_id": expected,
                "found_in_ledger": expected in event_ids,
            }
        write_event = next(
            (e for e in run.events if e.event_id == snapshot["write_event_id"]), None
        )
        checks["write_value_matches"] = (
            write_event is not None
            and write_event.payload.get("metric") == snapshot["metric"]
            and write_event.payload.get("value") == snapshot["value"]
        )
        checks["run_succeeded"] = run.status == RunStatus.SUCCEEDED
        checks["recovered_from"] = run.recovered_from
        verified = (
            all(item["found_in_ledger"] for item in [
                checks["start_event_id"], checks["write_event_id"], checks["confirm_event_id"]
            ])
            and checks["write_value_matches"]
            and checks["run_succeeded"]
        )
        return {"verified": verified, "checks": checks}

    # ---- 内部辅助 -------------------------------------------------------
    def _find_staging(self, run_id: str, metric: str) -> dict[str, Any] | None:
        for row in store.rows(STAGING_TABLE):
            if row["run_id"] == run_id and row["metric"] == metric:
                return row
        return None

    def _staging_of_run(self, run_id: str) -> list[dict[str, Any]]:
        return [row for row in store.rows(STAGING_TABLE) if row["run_id"] == run_id]


snapshot_catalog = SnapshotCatalog()
