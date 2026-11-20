"""聚合计算运行生命周期：事件流水、运行状态机与已确认指标快照。

- ledger：只追加的状态流水，开始/写入/结束事件全部带 run_id，可回放重建运行状态
- catalog：只发布完整且已确认的指标快照，部分写入或缺确认的数据无法从查询口读到
- service：对外编排一次聚合运行的完整生命周期（含失败恢复与迟到回执）
"""
from __future__ import annotations

from app.lifecycle.ledger import (
    RUN_TERMINAL_STATES,
    AggEvent,
    RunRecord,
    RunStatus,
    RunLedger,
)
from app.lifecycle.catalog import MetricSnapshot, SnapshotCatalog
from app.lifecycle.service import AggLifecycleService, LifecycleError

__all__ = [
    "RUN_TERMINAL_STATES",
    "AggEvent",
    "RunRecord",
    "RunStatus",
    "RunLedger",
    "MetricSnapshot",
    "SnapshotCatalog",
    "AggLifecycleService",
    "LifecycleError",
]
