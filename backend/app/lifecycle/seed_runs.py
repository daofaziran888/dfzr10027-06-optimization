"""为种子聚合任务播种真实运行历史。

store 初始化完成后调用：每个种子任务的状态都由真实事件流水回放得到，
包含完整成功、失败后关联恢复、等待确认三种形态，确保时间线与查询口
开箱即有可核对的数据，而不是按下标推算的假时间。
"""
from __future__ import annotations

from datetime import datetime, timedelta

from app.lifecycle.ledger import RunStatus, clock, run_ledger
from app.lifecycle.service import agg_lifecycle


def seed_agg_run_history() -> None:
    base = datetime(2026, 11, 20, 8, 0, 0)

    def at(minute: int) -> None:
        clock.pin(base + timedelta(minutes=minute))

    # 任务 1 AGG-GMV-DAILY：一次成功运行，gmv=40 已确认发布。
    at(0)
    agg_lifecycle.start_run(1)
    at(1); agg_lifecycle.write_metric(1, "gmv", 40)
    at(2); agg_lifecycle.complete_writes(1)
    at(3); agg_lifecycle.confirm_run(1)

    # 任务 2 AGG-UV-WEEKLY：写入中途失败（半成品 99 被拒绝），随后关联恢复成功发布 52。
    at(10)
    agg_lifecycle.start_run(2)
    at(11)
    agg_lifecycle.fail_run(2, reason="写入中途存储节点故障", partial_metric="gmv", partial_value=99)
    at(12)
    recovered = agg_lifecycle.start_run(2)  # recovered_from 关联失败运行
    at(13); agg_lifecycle.write_metric(2, "gmv", 52)
    at(14); agg_lifecycle.complete_writes(2)
    at(15); agg_lifecycle.confirm_run(2)
    assert recovered.recovered_from is not None

    # 任务 3 AGG-CVR-DAILY：一次成功运行。
    at(20)
    agg_lifecycle.start_run(3)
    at(21); agg_lifecycle.write_metric(3, "gmv", 71)
    at(22); agg_lifecycle.complete_writes(3)
    at(23); agg_lifecycle.confirm_run(3)

    # 任务 4 AGG-ARPU-WEEKLY：完整写入但完成回执超时，候选版本等待确认，查询口保持旧值。
    at(30)
    agg_lifecycle.start_run(4)
    at(31); agg_lifecycle.write_metric(4, "gmv", 58)
    at(32); agg_lifecycle.complete_writes(4)
    at(33); agg_lifecycle.mark_confirm_timeout(4)

    clock.resume()
    # 校验：四个任务都已有真实运行，且终态符合预期。
    for task_id in (1, 2, 3):
        assert run_ledger.latest_run(task_id).status == RunStatus.SUCCEEDED
    assert run_ledger.latest_run(4).status == RunStatus.TIMED_OUT
