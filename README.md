# 分析工坊 · 数据分析与数据应用平台

面向指标口径管理、多维度聚合查询、用户分群圈选、AB实验评估、时序趋势分析与数据服务发布的数据分析与数据应用平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 指标定义 | `metric_def` | 业务指标 | 指标编号、指标名称、计算公式 |
| 维度注册 | `dim_registry` | 分析维度 | 维度编号、维度名称、业务键 |
| 聚合任务 | `agg_task` | 聚合计算 | 任务编号、目标指标、聚合维度 |
| 多维立方体 | `cube_builder` | 数据立方体 | 立方体编号、事实来源、维度列表 |
| 分群规则 | `cohort_rule` | 用户分群 | 分群编号、分群名称、筛选条件 |
| 实验管理 | `experiment` | AB实验 | 实验编号、实验假设、分组方案 |
| 时序分析 | `trend_series` | 时序序列 | 序列编号、观测指标、时间粒度 |
| 归因模型 | `attribution` | 归因链路 | 链路编号、归因窗口、触点类型 |
| 数据分层 | `segmentation` | 数据切片 | 切片编号、源数据集、切分维度 |
| 报表生成 | `report_gen` | 分析报表 | 报表编号、报表类型、数据范围 |
| 预测分析 | `forecast` | 预测方案 | 方案编号、预测指标、预测算法 |
| 数据集市 | `data_mart` | 分析主题 | 主题编号、主题名称、业务域 |
| 统计建模 | `stat_model` | 统计模型 | 模型编号、模型类型、因变量 |
| 数据提取 | `data_extract` | 提取任务 | 任务编号、来源数据集、筛选条件 |
| 质量监控 | `quality_mon` | 监控规则 | 规则编号、监控对象、校验类型 |
| 对比分析 | `compare_job` | 对比任务 | 任务编号、基准数据、对比数据 |
| 数据服务 | `api_service` | 服务接口 | 接口编号、查询数据集、请求参数 |
| 调度编排 | `schedule_plan` | 调度计划 | 计划编号、关联任务、Cron表达式 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 聚合任务运行生命周期（事件流水）

聚合任务不再按列表位置推算时间，也不允许把计算失败直接标成完成。状态数据由
`backend/app/lifecycle/` 的只追加事件流水驱动：

- `ledger.py`：事件流水（`_agg_run_events`）。每次计算有唯一 `run_id`，
  `RUN_STARTED / METRIC_WRITTEN / WRITES_COMPLETED / RUN_FINISHED / RUN_FAILED /
  CONFIRM_TIMEOUT` 全部带真实时间戳与 `run_id`，运行状态由事件回放重建，非法流转直接拒绝。
- `catalog.py`：已确认指标快照目录（`_agg_metric_snapshots`）与暂存区
  （`_agg_snapshot_staging`）。发布门禁要求写齐全部指标且有真实完成回执；
  缺写入 / 部分指标的数据只留在暂存区（`rejected`），查询口读不到。
- `service.py`：生命周期命令（启动、写入、写完、确认、超时、失败、恢复）。
  从失败 / 超时状态恢复会新开一次运行，并通过 `recovered_from` 关联上一次运行。

时间线与查询口径：

- `GET /api/agg_task` 时间线的开始/结束时间与日志全部来自流水；未运行任务返回空时间。
- `GET /api/agg_task/{id}/runs`、`GET /api/agg_task/runs/{run_id}/events`
  可按任务或运行编号定位每次计算的开始/写入/结束记录。
- `GET /api/api_service/metrics/query?task_id=&metric=` 只返回已确认快照，
  带版本号、`run_id` 与开始/写入/结束事件编号，并在每次查询时回流水核对证据；
  结果完整但回执迟到时，查询口保持上一版本，同时通过 `pending_candidate`
  说明等待确认的候选版本。
- `GET /api/api_service/metrics/task/{task_id}` 返回任务下全部已确认指标及逐条核对结果。

场景模拟与验收：`backend/simulate_lifecycle.py`（旧值 40 / 新值 52，
覆盖回执超时与写入中途失败两个场景），后端测试见
`backend/tests/test_agg_lifecycle.py`。

领域：数据工程
子方向：数据分析与数据应用
