# 实时流数据处理中心

面向实时数据处理团队的流式计算与管理中心。覆盖流数据源接入、流处理算子编排、窗口计算配置、状态后端管理、检查点恢复、反压监控、延迟追踪与流作业运维的完整实时计算链路。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
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
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流数据源接入 | `streamin` | 流数据源 | 源编号、源名称、消息队列 |
| 流处理算子 | `operator` | 算子定义 | 算子编号、算子名称、输入流 |
| 窗口计算配置 | `window` | 窗口定义 | 窗口编号、数据流名称、窗口类型 |
| 状态后端管理 | `state` | 后端配置 | 配置编号、后端类型、存储路径 |
| 检查点恢复 | `checkpoint` | 检查点记录 / 恢复数据清单 | 记录编号、流作业名、检查点ID；目标实例、恢复耗时、恢复锁 |
| 反压监控面板 | `backpress` | 反压指标 | 指标编号、流作业名、反压级别 |
| 延迟追踪管理 | `latency` | 延迟记录 | 记录编号、流作业名、端到端延迟 |
| 流作业运维 | `jobops` | 作业实例 / 恢复清单消费记录 | 实例编号、作业名称、运行节点；恢复锁、消费处理记录 |
| 流数据输出端 | `sink` | 输出配置 | 配置编号、输出类型、目标地址 |
| 流数据格式定义 | `schema` | 格式定义 | 定义编号、流名称、字段名称 |
| 水位线管理 | `watermark` | 水位线配置 | 配置编号、流名称、时间字段 |
| 并行度调优 | `parallel` | 调优记录 | 记录编号、流算子名、当前并行度 |
| 流作业指标采集 | `metric` | 指标采集器 | 采集器编号、作业名称、指标名称 |
| 流告警规则 | `alarm` | 告警规则 | 规则编号、监控指标、告警条件 |
| 自定义函数库 | `udf` | 函数定义 | 函数编号、函数名称、输入参数 |
| 流处理日志中心 | `log` | 日志记录 | 记录编号、流作业名、日志级别 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 检查点恢复链路

检查点恢复不是单表增删，而是以「检查点恢复数据清单」为唯一事实来源：

1. `POST /api/checkpoint/{id}/restore` 从指定 CHEC 记录发起恢复，读取检查点ID、保存路径、
   状态大小，校验记录状态、恢复耗时和锁占用后，写入一份恢复清单；
   清单保留目标实例、恢复耗时（秒）、唯一恢复锁，并登记保存路径引用。
2. `GET /api/checkpoint/manifests` 读取清单；`POST /api/checkpoint/manifests/{恢复锁}/actions`
   推进 待处理 → 运行中 → 已恢复 → 已归档，`恢复失败` 则回到「已回退」，
   清单与保存路径引用一起退回，并写入回退记录（`GET /api/checkpoint/rollbacks`）。
3. 同一检查点同一时刻只有一把恢复锁；同一恢复请求号重复提交返回同一份清单与同一个目标实例。
4. `POST /api/jobops/manifests/{恢复锁}/consume` 让作业准备侧同步接收同一份清单，
   消费处理记录单独维护（`GET /api/jobops/consumptions/{恢复锁}`），不复制清单本身。
5. `POST /api/jobops/{id}/actions` 启动实例前核对清单状态、目标实例与保存路径占用，
   同一保存路径不允许两个实例同时持有。

回归测试：`cd backend && .venv/bin/python -m unittest discover -s tests -v`

领域：数据工程
子方向：数仓与数据湖
