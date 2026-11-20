"""聚合运行生命周期测试：事件流水、发布门禁、失败恢复与查询口证据。"""
from __future__ import annotations

import pytest

from app.lifecycle.catalog import PUBLISHED_TABLE, STAGING_TABLE, snapshot_catalog
from app.lifecycle.ledger import (
    EVENT_TABLE,
    RECOVERABLE_STATES,
    RunStatus,
    run_ledger,
)
from app.lifecycle.service import LifecycleError, agg_lifecycle
from app.services.agg_task import AggTaskService
from app.services.api_service import ApiServiceService
from app.store import store


@pytest.fixture()
def task_id() -> int:
    entry, missing = AggTaskService().create_entry({
        "任务编号": "AGG-TEST",
        "目标指标": "METR-GMV",
        "聚合维度": "DIM-REGION",
        "metrics": "gmv,orders",
    })
    assert not missing
    return int(entry["id"])


class TestEventLedger:
    def test_every_event_carries_run_id_and_real_timestamp(self, task_id):
        run = agg_lifecycle.start_run(task_id)
        agg_lifecycle.write_metric(task_id, "gmv", 40)
        events = run_ledger.events(task_id=task_id)
        assert events
        assert all(e.run_id == run.run_id for e in events)
        assert all(e.occurred_at for e in events)  # 不再按列表位置推算
        assert [e.seq for e in events] == list(range(1, len(events) + 1))

    def test_illegal_transition_rejected(self, task_id):
        # 没有启动运行就写入，必须拒绝。
        with pytest.raises(LifecycleError):
            agg_lifecycle.write_metric(task_id, "gmv", 1)

    def test_cannot_start_two_active_runs(self, task_id):
        agg_lifecycle.start_run(task_id)
        with pytest.raises(LifecycleError):
            agg_lifecycle.start_run(task_id)

    def test_failed_run_never_marked_completed(self, task_id):
        agg_lifecycle.start_run(task_id)
        agg_lifecycle.fail_run(task_id, reason="boom")
        run = run_ledger.latest_run(task_id)
        assert run.status == RunStatus.FAILED
        assert not any(e.event_type == "RUN_FINISHED" for e in run.events)
        # 任务行状态同步为真实失败状态，而不是“已完成”。
        assert store.find("agg_task", task_id)["status"] == "计算失败"


class TestPublishGate:
    def test_partial_write_is_not_published(self, task_id):
        agg_lifecycle.start_run(task_id)
        agg_lifecycle.write_metric(task_id, "gmv", 52)  # 只写了一个指标
        agg_lifecycle.fail_run(task_id, reason="写入中途失败")
        api = ApiServiceService()
        result = api.query_metric(task_id, "gmv")
        assert result["published"] is False
        assert result["value"] is None
        assert all(row["state"] == "rejected"
                   for row in store.rows(STAGING_TABLE))

    def test_complete_writes_without_confirm_not_queryable(self, task_id):
        agg_lifecycle.start_run(task_id)
        agg_lifecycle.write_metric(task_id, "gmv", 52)
        agg_lifecycle.write_metric(task_id, "orders", 5)
        agg_lifecycle.complete_writes(task_id)
        result = ApiServiceService().query_metric(task_id, "gmv")
        # 还没完成回执：查询口无已发布值，但能看到等待确认候选。
        assert result["published"] is False
        assert result["pending_candidate"]["state"] == "等待确认"
        assert result["pending_candidate"]["metrics"]["gmv"] == 52

    def test_complete_writes_requires_all_metrics(self, task_id):
        agg_lifecycle.start_run(task_id)
        agg_lifecycle.write_metric(task_id, "gmv", 52)
        with pytest.raises(LifecycleError):
            agg_lifecycle.complete_writes(task_id)


class TestTimeoutAndLateConfirm:
    def test_timeout_keeps_old_version_then_late_confirm_switches(self, task_id):
        api = ApiServiceService()
        # v1=40 已确认
        run1 = agg_lifecycle.start_run(task_id)
        agg_lifecycle.write_metric(task_id, "gmv", 40)
        agg_lifecycle.write_metric(task_id, "orders", 4)
        agg_lifecycle.complete_writes(task_id)
        agg_lifecycle.confirm_run(task_id)
        assert api.query_metric(task_id, "gmv")["value"] == 40

        # v2=52 完整写入但回执超时
        run2 = agg_lifecycle.start_run(task_id)
        agg_lifecycle.write_metric(task_id, "gmv", 52)
        agg_lifecycle.write_metric(task_id, "orders", 5)
        agg_lifecycle.complete_writes(task_id)
        agg_lifecycle.mark_confirm_timeout(task_id)

        waiting = api.query_metric(task_id, "gmv")
        assert waiting["version"] == "v1"
        assert waiting["value"] == 40  # 查询口仍旧版本
        assert waiting["pending_candidate"]["run_id"] == run2.run_id

        # 迟到回执
        agg_lifecycle.confirm_run(task_id)
        switched = api.query_metric(task_id, "gmv")
        assert switched["version"] == "v2"
        assert switched["value"] == 52
        assert switched["run_id"] == run2.run_id
        assert switched["evidence"]["verification"]["verified"] is True

    def test_timeout_is_recoverable(self, task_id):
        agg_lifecycle.start_run(task_id)
        agg_lifecycle.write_metric(task_id, "gmv", 1)
        agg_lifecycle.write_metric(task_id, "orders", 1)
        agg_lifecycle.complete_writes(task_id)
        timed_out = agg_lifecycle.mark_confirm_timeout(task_id)
        assert timed_out.status in RECOVERABLE_STATES
        recovered = agg_lifecycle.start_run(task_id)
        assert recovered.recovered_from == timed_out.run_id


class TestFailureRecovery:
    def test_recovery_creates_linked_run(self, task_id):
        failed = agg_lifecycle.start_run(task_id)
        agg_lifecycle.fail_run(task_id, reason="存储故障",
                               partial_metric="gmv", partial_value=52)
        recovered = agg_lifecycle.start_run(task_id)
        assert recovered.recovered_from == failed.run_id
        assert recovered.attempt == failed.attempt + 1
        # 恢复运行的首个事件携带关联。
        first_event = run_ledger.events(run_id=recovered.run_id)[0]
        assert first_event.recovered_from == failed.run_id

    def test_recovered_run_publishes_with_full_evidence_chain(self, task_id):
        agg_lifecycle.start_run(task_id)
        agg_lifecycle.fail_run(task_id, reason="x", partial_metric="gmv", partial_value=9)
        recovered = agg_lifecycle.start_run(task_id)
        agg_lifecycle.write_metric(task_id, "gmv", 52)
        agg_lifecycle.write_metric(task_id, "orders", 5)
        agg_lifecycle.complete_writes(task_id)
        agg_lifecycle.confirm_run(task_id)

        result = ApiServiceService().query_metric(task_id, "gmv")
        assert result["value"] == 52
        assert result["run_id"] == recovered.run_id
        assert result["recovered_from"] is not None
        chain = result["evidence"]["event_chain"]
        assert len(chain) == 3 and all(chain)
        assert result["evidence"]["verification"]["verified"] is True


class TestQueryEvidence:
    def test_query_explains_which_run_and_version(self, task_id):
        run = agg_lifecycle.start_run(task_id)
        agg_lifecycle.write_metric(task_id, "gmv", 40)
        agg_lifecycle.write_metric(task_id, "orders", 4)
        agg_lifecycle.complete_writes(task_id)
        agg_lifecycle.confirm_run(task_id)

        result = ApiServiceService().query_metric(task_id, "gmv")
        assert result["version"] == "v1"
        assert result["run_id"] == run.run_id
        assert result["run_status"] == "已完成"
        checks = result["evidence"]["verification"]["checks"]
        assert checks["run_succeeded"] is True
        assert checks["write_value_matches"] is True

    def test_task_snapshots_only_confirmed(self, task_id):
        agg_lifecycle.start_run(task_id)
        agg_lifecycle.write_metric(task_id, "gmv", 40)
        agg_lifecycle.write_metric(task_id, "orders", 4)
        agg_lifecycle.complete_writes(task_id)
        agg_lifecycle.confirm_run(task_id)
        view = ApiServiceService().query_task_snapshots(task_id)
        assert view["published_count"] == 2
        assert all(item["verification"]["verified"] for item in view["items"])
        # 发布表中的活动快照全部来自成功运行。
        active = [r for r in store.rows(PUBLISHED_TABLE) if r["active"]]
        assert len(active) == 2
        assert all(run_ledger.get_run(r["run_id"]).status == RunStatus.SUCCEEDED
                   for r in active)

    def test_events_persisted_in_dedicated_table(self, task_id):
        agg_lifecycle.start_run(task_id)
        assert len(store.rows(EVENT_TABLE)) >= 1
