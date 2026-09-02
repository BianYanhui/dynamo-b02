# B02 Sketch Router — B02 语义状态接口融入 Dynamo

> Fork: BianYanhui/dynamo-b02 · 分支: `b02-fusion` · 基于 Dynamo 1.4.2 运行时 (PyPI wheel) 开发

## 这是什么

把 B02 的核心思想——**Instance–Dispatcher 边界应当暴露代价感知的语义状态视图**——
实现为 Dynamo 的一个原生路由组件：

```
Client ──HTTP──> Frontend ──预处理──> B02SketchRouter ──direct/pin──> vLLM workers (4×T4)
                    │                    │
                    │              ┌─────┴──────────────────────────┐
                    │              │ 1. WorkflowTable  (B02 §11)     │
                    │              │ 2. Sketch-Dispatch (B02 §1.2)   │
                    │              │ 3. Coarse/Rich/Sketch 视图字节   │
                    │              │    会计 (B02 §1.4, 冻结字段)      │
                    │              └────────────────────────────────┘
```

- **工作流身份**：客户端带 `x-dynamo-session-id` 头 → 前端注入 `agent_context.session_id`
  → router 以其作为 workflow_id（无头请求走原生 KvRouter 透传）
- **Sketch-Dispatch**（B02 design.md §1.2）：`score(I) = α·inflight(I) + γ·affinity(I,R)`，
  亲和项来自 WorkflowTable（该工作流的上一个步骤落在哪个实例）；命中亲和且实例
  未过载 → 以 `routing.backend_instance_id` 钉住（Dynamo 原生钉定机制）；否则交给
  原生 KvRouter（保留 Dynamo 的 KV-overlap 能力）并记录实际选择
- **三视图字节会计**（B02 design.md §1.4 冻结字段）：每个 tick 对每个实例构建
  Coarse / Rich / Sketch 三种状态视图并测量序列化字节数，写入
  `state_updates.jsonl`——B02 的头条度量（Sketch≈Coarse≪Rich）在工业系统内复现

## 与 ThunderAgent 的关系

接线模式（`KvRouter` 包装、`register_model` 注册模型面、`routing.backend_instance_id`
钉定、chunk 中 `routing_data.worker_id` 归因）复用 `dynamo.thunderagent_router` 的
公开模式；策略与状态会计完全来自 B02。

## 目录

```
b02/
├── README.md                  # 本文件
├── b02_sketch_router/
│   ├── __main__.py            # 服务接线（Dynamo 组件）
│   ├── workflow_state.py      # WorkflowRecord / WorkflowTable (B02 §11)
│   ├── state_views.py         # Coarse/Rich/Sketch 构建器 + 字节会计 (B02 §1.4)
│   └── policy.py              # Sketch-Dispatch 评分 (B02 §1.2)
├── tests/test_state_views.py  # 视图构建器单测（纯 Python）
└── smoke/
    ├── run_smoke.sh           # 一键冒烟：集群 → router → 负载 → 分析
    ├── smoke_client.py        # agentic 负载客户端（带会话头）
    └── analyze_smoke.py       # 粘性/复用/视图字节 汇总
```

## 运行

```bash
bash /home/byh/Dynamo/cluster_up.sh                 # 4×vLLM worker + frontend
bash /home/byh/Dynamo/dynamo/b02/smoke/run_smoke.sh # router + 冒烟 + 分析
bash /home/byh/Dynamo/cluster_down.sh               # 收尾释放 GPU
```

冒烟判定（详见 smoke/README 注释）：
1. 全部请求 200 且响应合法
2. 同工作流步骤粘性（decisions.jsonl 中 source=pin 比例 & worker 一致性）
3. `state_updates.jsonl`：sketch_bytes ≈ coarse_bytes ≪ rich_bytes（B02 头条复现）
4. cached_tokens 随步骤增长（前缀复用经钉定路由得以发生）
