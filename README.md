# FastAPI Service Hardening (Python)

在原有 `GET / -> {"Hello": "World"}` 服务的基础上，增加 **并发与准入控制**、
**下游超时与隔离**、**全链路可观测性**，在高并发与慢依赖场景下保持稳定，
同时对原有行为完全向后兼容。

---

## 1. 架构总览

```
                 ┌────────────────────────────────────────────┐
   HTTP 请求 ──▶ │ /call                                       │
                 │  ① 跨进程固定窗口限流 (flock + 状态文件)     │  429 rate_limited
                 │  ② 进程内隔离舱 Bulkhead (信号量)            │  503 bulkhead_full
                 │  ③ 有界共享 HTTP 连接池 + 硬超时             │  504 upstream_timeout
                 │                                            │  502 upstream_error
                 │  ④ 指标记录 (计数 + 时延直方图)             │  500 internal_error
                 └────────────────────────────────────────────┘
```

模块：

| 文件 | 职责 |
| --- | --- |
| `main.py` | 应用装配、路由、准入顺序、统一错误信封、生命周期 |
| `config.py` | 环境变量配置（全部带安全默认值） |
| `limiter.py` | 跨进程固定窗口限流器 + 进程内隔离舱 |
| `clients.py` | 有界共享 HTTP 客户端池，硬超时 + 连接隔离 |
| `downstream.py` | 下游调用，超时/错误异常分类 |
| `metrics.py` | 计数器、时延直方图、快照、周期 JSON 日志 |
| `logging_setup.py` | 结构化日志配置 |

`main.py`（仓库根）仅 `from app.main import app`，因此
`uvicorn main:app` 以及任何已有的 `main.app` 引用保持可用。

---

## 2. 并发与准入策略

准入分两层，**失败原因严格可区分**，且先做最便宜的检查：

### 2.1 跨进程固定窗口限流（HTTP 429）

* 每个工作进程通过同一个状态文件 + `fcntl.flock` 排他锁协调；
  同一台主机上无论起多少个 uvicorn worker，总速率不超过配额。
* 固定 1 秒窗口：窗口内计数达到 `RATE_LIMIT_PER_SECOND` 即拒绝，
  窗口滚动后配额恢复。
* 只有**超出承载能力**的请求被拒绝（429）；配额内的正常请求不会被误伤。
* 限流判定在隔离舱之前，被拒请求不会占用下游连接或并发槽。
* 阻塞式的 flock + 文件写入通过 `asyncio.to_thread` 在线程中执行，
  不阻塞事件循环。
* 状态文件异常时限流器 **fail-open**（限流存储故障不应放大为整体宕机）。

### 2.2 进程内隔离舱 Bulkhead（HTTP 503）

* 进程内非阻塞信号量，限制同时进行的下游调用数（`MAX_CONCURRENT_CALLS`）。
* 满员时立即返回 503 `bulkhead_full`，不排队——排队只会掩盖过载。
* 慢依赖在每个进程内最多占用该数量的槽位，无法耗尽事件循环。

> 多进程一致性说明：文件锁方案保证单机多 worker 的速率配额一致；
> 隔离舱是**每进程**预算（进程之间本就不共享事件循环/连接）。

### 拒绝语义对照

| 场景 | 状态码 | `error.type` |
| --- | --- | --- |
| 超过跨进程速率配额 | 429 | `rate_limited` |
| 本进程并发槽耗尽 | 503 | `bulkhead_full` |
| 下游在期限内未响应 | 504 | `upstream_timeout` |
| 下游连接失败/4xx/5xx/非 JSON | 502 | `upstream_error` |
| 未预期内部错误 | 500 | `internal_error` |

所有失败响应统一信封：`{"error": {"type": ..., "message": ...}}`。

---

## 3. 超时与隔离语义

* 每次下游调用都在 `anyio.fail_after(UPSTREAM_TIMEOUT_SECONDS)` 取消作用域内：
  到点后请求被**真正取消**，而不是挂起或静默降级；调用方拿到明确的 504。
* 使用**有界共享连接池**复用热连接（避免每请求新建 TCP）；
  当请求被取消时，其响应流被销毁，对应 socket 会被 httpx 关闭而**不会**
  放回 keepalive 集合，因此半死连接不会污染后续请求
  （见 `test_cancelled_connections_do_not_poison_pool`）。
* 信号量 + 隔离舱双重有界：慢依赖最多持有 `MAX_CONCURRENT_CALLS` 个槽位。
* 应用关闭时调用 `pool.aclose()` 释放全部 socket 与工作线程。
* 连接建立阶段单独受限（`UPSTREAM_CONNECT_TIMEOUT_SECONDS`），
  总耗时由每次调用的硬期限约束。

---

## 4. 可观测性

### 4.1 `GET /metrics`（拉取）

返回进程生命周期内的聚合 JSON：

* `counts`：六种结果（ok / timeout / rate_limited / bulkhead_full /
  upstream_error / internal_error）的请求计数；
* `latencies`：每种结果的时延直方图与 `count / sum / avg / max / p50 / p95 / p99`，
  桶上界单位秒（0.005 … 10s，亚秒区间更细）；
* `in_flight`：当前在处理的 `/call` 数（任何路径都会递减）；
* `accepted` / `rejected`：接受（含下游失败）与准入拒绝计数；
* `completed`、`uptime_seconds` 等。

**冷启动语义一致**：进程刚启动、零请求时 `/metrics` 即返回结构完整的零值，
预热前后字段含义完全相同，无需特殊处理。

### 4.2 结构化日志（推送）

* 每个 `/call` 结束打印一行 `request_done {json}`：结果、时延、在途数、url；
* 每隔 `METRICS_EMIT_INTERVAL_SECONDS` 打印一行
  `metrics_snapshot {json}`，便于日志管道按进程聚合吞吐与时延；
* 设为 `0` 关闭周期输出（`/metrics` 仍然可用）。

---

## 5. 配置（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `RATE_LIMIT_PER_SECOND` | `10` | 跨进程每秒速率配额 |
| `RATE_LIMIT_STATE_DIR` | 系统临时目录 | 限流状态文件目录（同机 worker 必须相同） |
| `MAX_CONCURRENT_CALLS` | `20` | 每进程下游并发槽（隔离舱） |
| `UPSTREAM_TIMEOUT_SECONDS` | `1.0` | 单次下游调用硬超时 |
| `UPSTREAM_CONNECT_TIMEOUT_SECONDS` | `0.5` | 建连阶段超时 |
| `CLIENT_POOL_SIZE` | `0`(=隔离舱容量) | 共享连接池 lane 数 |
| `PER_CLIENT_CONNECTIONS` | `8` | 每个 client 的最大连接数 |
| `METRICS_EMIT_INTERVAL_SECONDS` | `5.0` | 周期指标日志间隔，`0` 关闭 |

---

## 6. 路由

* `GET /`：原有契约，响应体保持 `{"Hello":"World"}` 不变。
* `GET /health`：存活探针。
* `GET /call?url=<下游地址>`：带全部加固能力的下游调用。
* `GET /mock/delay?seconds=<n>`：内置慢依赖（本地验证用）。
* `GET /mock/error`：内置 500 依赖（验证错误分类）。
* `GET /metrics`：聚合指标。

---

## 7. 本地验证

### 启动

```bash
uv sync
RATE_LIMIT_PER_SECOND=5 \
MAX_CONCURRENT_CALLS=4 \
UPSTREAM_TIMEOUT_SECONDS=0.3 \
uv run uvicorn main:app --host 127.0.0.1 --port 8000
```

### 手动场景

```bash
# 正常（200）
curl 'http://127.0.0.1:8000/call?url=http%3A%2F%2F127.0.0.1%3A8000%2Fmock%2Fdelay%3Fseconds%3D0.05'

# 慢依赖 -> 约 0.3s 返回 504 upstream_timeout（不挂起）
curl 'http://127.0.0.1:8000/call?url=http%3A%2F%2F127.0.0.1%3A8000%2Fmock%2Fdelay%3Fseconds%3D2'

# 下游错误 -> 502 upstream_error
curl 'http://127.0.0.1:8000/call?url=http%3A%2F%2F127.0.0.1%3A8000%2Fmock%2Ferror'

# 指标
curl 'http://127.0.0.1:8000/metrics'
```

### 多 worker 一致性

```bash
RATE_LIMIT_PER_SECOND=20 RATE_LIMIT_STATE_DIR=/tmp/rl \
uv run uvicorn main:app --workers 4 --host 127.0.0.1 --port 8000
# 4 个 worker 共享 20/s 的总配额（而非各自 20/s）
```

### 测试

```bash
uv run pytest -s          # -s 可查看并发规模、时延分布、拒绝计数打印
```

测试会启动**真实 uvicorn 子进程**（真实 socket 才能复现连接池、超时与文件锁），
覆盖：

* 慢依赖：20 个"卡死"调用全部在约超时点返回 504，且可区分；
* **吞吐 A/B**：对比 `tests/baseline_app.py`（无超时、共享受限连接池的改造前版本），
  断言改造后吞吐有可复现提升（典型约 1.9x）；
* 资源回收：24 次超时后 `in_flight=0`，后续正常调用全部 200；
* 连接池防污染：慢超时与快速成功混合，快速调用始终 200 且不被拖累；
* 隔离舱：过载并发出现可区分的 503 `bulkhead_full`；
* 限流：配额内零误伤、跨进程文件锁一致性、双 worker 共享配额；
* 指标：冷启动零值、计数与时延对账一致。

每个压测用例都会打印并发规模、各状态码 p50/p95/p99/max 时延分布与拒绝计数。
