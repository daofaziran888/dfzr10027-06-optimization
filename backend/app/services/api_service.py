"""数据服务业务规则：服务接口管理 + 已确认指标快照查询。

查询口径
--------
指标查询只走 :mod:`app.lifecycle` 的快照目录：

* 部分写入 / 缺写入的数据从未通过发布门禁，查询口读不到；
* 结果完整但完成回执迟到时，查询口继续返回上一已确认版本，
  同时显式告知“有一个等待确认的候选版本”；
* 每个返回值都带 run_id 与开始/写入/结束事件编号，并在每次查询时
  回到事件流水重新核对，说明这个值对应哪次运行、证据是否仍然成立。
"""
from __future__ import annotations

from typing import Any

from app.lifecycle.catalog import snapshot_catalog
from app.lifecycle.ledger import run_ledger
from app.store import store

MODULE = "api_service"
LIST_FIELDS = ["接口编号", "查询数据集", "请求参数", "返回字段", "缓存秒数", "QPS上限", "认证方式", "版本号"]
REQUIRED_FIELDS = ["接口编号", "查询数据集", "请求参数"]
STATUS_ORDER = ["草稿", "已发布", "已下线", "已废弃"]
ACTION_RULES = {"发布服务": "已发布", "下线服务": "已下线", "废弃服务": "已废弃"}
NEGATIVE_ACTIONS = ["下线服务"]


class ApiServiceService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("接口编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"服务接口 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据服务可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"服务接口已{action}"

    # ---- 指标快照查询（沿流水核对证据） ----------------------------------
    def query_metric(self, task_id: int, metric: str) -> dict[str, Any]:
        snapshot = snapshot_catalog.get_published(task_id, metric)
        if snapshot is None:
            # 没有已确认版本：明确区分“从未发布”和“有半成品但未过门禁”。
            candidate = snapshot_catalog.pending_candidate(task_id)
            return {
                "task_id": task_id,
                "metric": metric,
                "published": False,
                "value": None,
                "reason": "没有通过发布门禁的已确认快照（缺写入或完成回执未确认）",
                "evidence": None,
                "pending_candidate": candidate,
            }

        row = snapshot.as_dict()
        verification = snapshot_catalog.verify_evidence(row)
        run = run_ledger.get_run(row["run_id"])
        return {
            "task_id": task_id,
            "metric": metric,
            "published": True,
            "value": row["value"],
            "version": row["version_label"],
            "version_no": row["version"],
            "run_id": row["run_id"],
            "attempt": row["attempt"],
            "recovered_from": row.get("recovered_from"),
            "published_at": row["published_at"],
            "previous_version": row.get("previous_version"),
            "evidence": {
                "event_chain": row["event_chain"],
                "start_event_id": row["start_event_id"],
                "write_event_id": row["write_event_id"],
                "confirm_event_id": row["confirm_event_id"],
                "verification": verification,
            },
            "run_status": run.status.value if run else None,
            # 当前是否有更新的候选版本仍在等待确认（含回执超时场景）。
            "pending_candidate": snapshot_catalog.pending_candidate(task_id),
        }

    def query_task_snapshots(self, task_id: int) -> dict[str, Any]:
        """一次返回任务下全部已发布指标，并逐条附带流水证据核对结果。"""
        snapshots = [snapshot.as_dict() for snapshot in snapshot_catalog.list_published(task_id)]
        items = []
        for row in snapshots:
            items.append({
                "metric": row["metric"],
                "value": row["value"],
                "version": row["version_label"],
                "run_id": row["run_id"],
                "event_chain": row["event_chain"],
                "verification": snapshot_catalog.verify_evidence(row),
            })
        return {
            "task_id": task_id,
            "published_count": len(items),
            "items": items,
            "pending_candidate": snapshot_catalog.pending_candidate(task_id),
        }
