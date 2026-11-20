#!/usr/bin/env python3
"""聚合运行生命周期场景模拟（题目验收脚本）。

场景 A：结果完整但完成回执超时
    旧聚合值 gmv=40 已确认发布；新一轮算出 gmv=52 且全部指标完整写入，
    但完成回执迟到 -> 时间线显示“回执超时”，查询口仍返回 v1=40，
    并标注存在等待确认的候选版本；迟到回执到达后查询口才切换到 v2=52。

场景 B：写入中途失败 + 失败恢复
    新一轮只写入部分指标即失败 -> 半成品不发布、查询口保持 v2=52；
    从失败状态恢复产生关联运行（recovered_from 指向上次失败 run），
    新运行写齐并确认后发布 v3。

每一步都通过数据服务查询口核对：指标版本、值、对应 run_id 与流水证据链。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from app.lifecycle.catalog import snapshot_catalog
from app.lifecycle.ledger import RunStatus, clock, run_ledger
from app.lifecycle.service import agg_lifecycle
from app.services.agg_task import AggTaskService
from app.services.api_service import ApiServiceService
from app.store import reset_store, store

BASE_TIME = datetime(2026, 11, 20, 9, 0, 0)


class Recorder:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self.assertions: list[tuple[str, bool, str]] = []

    def log(self, text: str) -> None:
        self.lines.append(text)

    def check(self, name: str, condition: bool, detail: str = "") -> None:
        self.assertions.append((name, bool(condition), detail))
        mark = "PASS" if condition else "FAIL"
        self.log(f"  [{mark}] {name}" + (f" -> {detail}" if detail else ""))

    def report(self) -> str:
        passed = sum(1 for _, ok, _ in self.assertions if ok)
        total = len(self.assertions)
        self.lines.append("")
        self.lines.append(f"核对汇总：{passed}/{total} 项通过")
        return "\n".join(self.lines)


def tick(minutes: int = 1) -> None:
    """推进仿真时钟。"""
    if clock._fixed is None:
        clock.pin(BASE_TIME)
    else:
        clock.pin(clock._fixed + timedelta(minutes=minutes))


def create_task(metrics: list[str]) -> int:
    entry, missing = AggTaskService().create_entry({
        "任务编号": "AGG-SIM",
        "目标指标": "METR-GMV",
        "聚合维度": "DIM-REGION",
        "metrics": ",".join(metrics),
    })
    assert not missing
    return int(entry["id"])


def show_query(rec: Recorder, api: ApiServiceService, task_id: int, metric: str,
               title: str) -> dict:
    result = api.query_metric(task_id, metric)
    rec.log(f"  查询 {title}: " + json.dumps({
        "published": result["published"],
        "value": result.get("value"),
        "version": result.get("version"),
        "run_id": result.get("run_id"),
        "verified": (result.get("evidence") or {}).get("verification", {}).get("verified"),
        "pending": (result.get("pending_candidate") or {}).get("state"),
    }, ensure_ascii=False))
    return result


def scenario_a(rec: Recorder) -> None:
    rec.log("=" * 78)
    rec.log("场景 A：结果完整写入但完成回执超时（旧值 40 -> 新值 52）")
    rec.log("=" * 78)
    api = ApiServiceService()
    task_id = create_task(["gmv", "orders"])

    # 基线运行：gmv=40，正常确认，发布 v1。
    tick()
    run1 = agg_lifecycle.start_run(task_id)
    tick(); agg_lifecycle.write_metric(task_id, "gmv", 40)
    tick(); agg_lifecycle.write_metric(task_id, "orders", 4)
    tick(); agg_lifecycle.complete_writes(task_id)
    tick(); agg_lifecycle.confirm_run(task_id)
    rec.log(f"基线运行 {run1.run_id} 已确认发布 v1（gmv=40, orders=4）")

    q = show_query(rec, api, task_id, "gmv", "基线")
    rec.check("基线版本为 v1 且值=40", q["version"] == "v1" and q["value"] == 40,
              f'{q["version"]}={q["value"]}')
    rec.check("基线查询流水证据核对通过", q["evidence"]["verification"]["verified"] is True)
    rec.check("基线值可定位到运行", q["run_id"] == run1.run_id, q["run_id"])

    # 第二轮：gmv=52，完整写入但回执超时。
    tick()
    run2 = agg_lifecycle.start_run(task_id)
    tick(); agg_lifecycle.write_metric(task_id, "gmv", 52)
    tick(); agg_lifecycle.write_metric(task_id, "orders", 5)
    tick(); agg_lifecycle.complete_writes(task_id)
    tick(); agg_lifecycle.mark_confirm_timeout(task_id)
    rec.log(f"新运行 {run2.run_id} 完整写入 gmv=52 后完成回执超时")

    timeline = agg_lifecycle.task_timeline(task_id)
    rec.check("时间线最新运行状态为回执超时",
              timeline["current_status"] == RunStatus.TIMED_OUT.value,
              timeline["current_status"])
    rec.check("时间线能看到两个版本的运行", timeline["run_count"] == 2)

    q2 = show_query(rec, api, task_id, "gmv", "超时后")
    rec.check("超时后查询口仍返回旧版本 v1=40",
              q2["version"] == "v1" and q2["value"] == 40, f'{q2["version"]}={q2["value"]}')
    rec.check("查询口告知存在等待确认的候选（52）",
              q2["pending_candidate"] is not None
              and q2["pending_candidate"]["metrics"].get("gmv") == 52
              and q2["pending_candidate"]["state"] == RunStatus.TIMED_OUT.value)
    # 时间线与查询口看到不同版本，业务能明确“现在可查到的是哪次聚合”。
    rec.check("时间线候选 run 与查询口 run 不同（版本分叉可见）",
              q2["pending_candidate"]["run_id"] == run2.run_id and q2["run_id"] == run1.run_id)

    # 迟到回执到达。
    tick()
    agg_lifecycle.confirm_run(task_id)
    rec.log(f"迟到完成回执到达，{run2.run_id} 转为已完成并发布 v2")
    q3 = show_query(rec, api, task_id, "gmv", "迟到回执后")
    rec.check("迟到回执后查询口切换到 v2=52",
              q3["version"] == "v2" and q3["value"] == 52, f'{q3["version"]}={q3["value"]}')
    rec.check("v2 证据链包含开始/写入/结束三个事件",
              len(q3["evidence"]["event_chain"]) == 3
              and all(q3["evidence"]["verification"]["checks"][k]["found_in_ledger"]
                      for k in ("start_event_id", "write_event_id", "confirm_event_id")))
    rec.check("v2 写入事件中的值与查询值一致",
              q3["evidence"]["verification"]["checks"]["write_value_matches"] is True)
    rec.check("确认后无等待中的候选版本", q3["pending_candidate"] is None)


def scenario_b(rec: Recorder) -> None:
    rec.log("")
    rec.log("=" * 78)
    rec.log("场景 B：写入中途失败，随后从失败状态恢复重跑")
    rec.log("=" * 78)
    api = ApiServiceService()
    task_id = create_task(["gmv", "orders", "aov"])

    # 先建立已确认基线 v1。
    tick()
    base = agg_lifecycle.start_run(task_id)
    tick(); agg_lifecycle.write_metric(task_id, "gmv", 40)
    tick(); agg_lifecycle.write_metric(task_id, "orders", 4)
    tick(); agg_lifecycle.write_metric(task_id, "aov", 10)
    tick(); agg_lifecycle.complete_writes(task_id)
    tick(); agg_lifecycle.confirm_run(task_id)
    rec.log(f"基线运行 {base.run_id} 已确认发布 v1（gmv=40）")

    # 失败运行：只写了 gmv=52 就中断。
    tick()
    failed = agg_lifecycle.start_run(task_id)
    tick()
    agg_lifecycle.fail_run(task_id, reason="写入中途存储节点故障",
                           partial_metric="gmv", partial_value=52)
    rec.log(f"运行 {failed.run_id} 写入中途失败，仅留下 gmv=52 的半成品")

    timeline = agg_lifecycle.task_timeline(task_id)
    rec.check("失败运行状态为计算失败（不会被标成完成）",
              timeline["current_status"] == RunStatus.FAILED.value, timeline["current_status"])
    failed_run = run_ledger.get_run(failed.run_id)
    rec.check("失败事件真实落流水", failed_run.end_event_id is not None
              and failed_run.failure_reason == "写入中途存储节点故障")

    q = show_query(rec, api, task_id, "gmv", "失败后")
    rec.check("半成品未发布，查询口保持 v1=40",
              q["version"] == "v1" and q["value"] == 40, f'{q["version"]}={q["value"]}')
    q_orders = api.query_metric(task_id, "orders")
    rec.check("缺写入的 orders 仍可查到旧已确认版本", q_orders["value"] == 4)
    rec.check("失败后不存在等待确认候选（半成品已拒绝）", q["pending_candidate"] is None)

    # 从失败状态恢复：产生关联运行。
    tick()
    recovered = agg_lifecycle.start_run(task_id)
    rec.log(f"从失败状态恢复，开启关联运行 {recovered.run_id}（recovered_from={failed.run_id}）")
    rec.check("恢复运行通过 recovered_from 关联失败运行",
              recovered.recovered_from == failed.run_id, recovered.recovered_from or "")
    rec.check("恢复运行是第 3 次尝试（attempt=3，前两次为基线与失败运行）",
              recovered.attempt == 3, str(recovered.attempt))

    tick(); agg_lifecycle.write_metric(task_id, "gmv", 52)
    tick(); agg_lifecycle.write_metric(task_id, "orders", 5)
    tick(); agg_lifecycle.write_metric(task_id, "aov", 13)
    tick(); agg_lifecycle.complete_writes(task_id)
    tick(); _, published = agg_lifecycle.confirm_run(task_id)
    rec.log(f"恢复运行写齐 3 个指标并确认，发布 {len(published)} 条 v2 快照")

    q2 = show_query(rec, api, task_id, "gmv", "恢复后")
    rec.check("恢复后查询口发布 v2=52", q2["version"] == "v2" and q2["value"] == 52,
              f'{q2["version"]}={q2["value"]}')
    rec.check("v2 指向恢复运行而非失败运行",
              q2["run_id"] == recovered.run_id and q2["run_id"] != failed.run_id, q2["run_id"])
    rec.check("v2 证据保留 recovered_from 关联", q2["recovered_from"] == failed.run_id)
    rec.check("v2 流水证据核对通过", q2["evidence"]["verification"]["verified"] is True)

    # 任务级快照：全部指标都只来自已确认版本。
    task_view = api.query_task_snapshots(task_id)
    rec.check("任务下 3 个指标全部有已确认快照", task_view["published_count"] == 3,
              str(task_view["published_count"]))
    rec.check("任务级逐条证据核对全部通过",
              all(item["verification"]["verified"] for item in task_view["items"]))

    # 失败运行的半成品在暂存区是 rejected，发布表里没有它的 run_id。
    published_run_ids = {row["run_id"] for row in store.rows("_agg_metric_snapshots")}
    rec.check("失败运行从未进入发布表", failed.run_id not in published_run_ids)
    staging_states = {row["state"] for row in store.rows("_agg_snapshot_staging")
                      if row["run_id"] == failed.run_id}
    rec.check("失败运行暂存数据标记为 rejected", staging_states == {"rejected"}, str(staging_states))


def main() -> int:
    reset_store()
    clock.pin(BASE_TIME)
    rec = Recorder()
    scenario_a(rec)
    scenario_b(rec)

    # 额外断言：时间线不再依赖列表位置——事件时间全部来自仿真时钟且单调。
    rec.log("")
    rec.log("=" * 78)
    rec.log("附加核对：时间来自真实事件而非列表位置")
    rec.log("=" * 78)
    for event in run_ledger.events():
        rec.log(f"  {event.occurred_at} {event.event_type} task={event.task_id} run={event.run_id}")
    seqs = [e.seq for e in run_ledger.events()]
    rec.check("流水事件序号连续且无缺口", seqs == list(range(1, len(seqs) + 1)))
    times = [datetime.fromisoformat(e.occurred_at) for e in run_ledger.events()]
    rec.check("事件时间戳单调不减", all(times[i] <= times[i + 1] for i in range(len(times) - 1)))
    rec.check("每条事件都能定位到具体运行", all(e.run_id for e in run_ledger.events()))

    print(rec.report())
    return 0 if all(ok for _, ok, _ in rec.assertions) else 1


if __name__ == "__main__":
    raise SystemExit(main())
