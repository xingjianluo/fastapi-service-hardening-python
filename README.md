# FastAPI Service Hardening

在最小 FastAPI 骨架（`GET / -> {"Hello": "World"}`）的基础上，叠加**并发准入控制、
下游隔离与超时、全链路观测**三层能力，并保持原有接口与响应完全向后兼容。

## 目录结构

```
main.py                # 仅暴露 app = create_app()
app/config.py          # 全部可调参数（环境变量覆盖）
app/state.py           # 共享 SQLite（WAL）连接与表结构
app/rate_limit.py      # 跨进程令牌桶（BEGIN IMMEDIATE 串行化）
app/downstream.py     # 超时 + 并发舱壁（Semaphore + httpx 连接池）
app/metrics.py         # 结果分类计数、时延统计与对数分桶 p50/p90/p99
app/outcomes.py        # 五类结果枚举
app/http_app.py        # 装配、端点、失败语义与埋点
tests/                 # 真实 uvicorn 子进程 + 并发洪泛 + 报告
```

## 运行

```bash
uv sync --group dev          # 安装运行与测试依赖
uv run uvicorn main:app \
  --host 127.0.0.1 --port 8000 --workers 2
```

冷启动时会在 `SERVICE_DB_PATH`（默认 `.state/service.db`）自动建库建表。

## 端点

| 路径 | 说明 |
| --- | --- |
| `GET /` | 原始骨架响应，未改动，向后兼容 |
| `GET /data?target=<url>` | 加固路径：限流 → 舱壁 → 带超时的下游调用 |
| `GET /upstream-legacy?target=<url>` | **改造前**对照路径：同步阻塞、无超时、无隔离（用于量化改善） |
| `GET /internal/downstream?delay=&fail=` | 内置可控慢/故障依赖，便于本地验证 |
| `GET /metrics` | 跨进程聚合的吞吐与时延快照 |

`/data` 不传 `target` 时默认调用内置依赖（50ms 延迟），开箱即用。

## 一、并发与准入策略（Rate Limiting）

采用**跨进程共享的令牌桶**，状态存于 WAL 模式的 SQLite：

* 每个请求在一个 `BEGIN IMMEDIATE` 事务内完成「读旧状态 → 惰性补充 → 扣减」，
  进程内用锁串行化、进程间由 SQLite 写锁串行化，因此多 worker 下**绝不会超发**。
* **正常流量不被误伤**：桶非空即放行，突发最多接纳 `capacity` 个；超容量请求
  **不消耗令牌**，立即返回可区分的 `429 rate_limited`。
* 令牌按 `refill_per_sec` 线性恢复；因此系统对短暂突发友好，对持续过载有明确拒绝语义。

## 二、超时与隔离语义（Timeout & Bulkhead）

对下游调用同时施加两道保护：

1. **硬超时**：每个调用包裹 `asyncio.timeout(timeout)`，叠加 httpx 自身超时。
   超时不会挂起、也不会被静默吞掉，而是转成 **`504 downstream_timeout`**，
   时延被钉死在超时阈值附近（而非依赖的真实耗时）。
2. **并发舱壁**：`asyncio.Semaphore(max_inflight)` 限制在途调用数，httpx 连接池
   同样以该值封顶。舱壁满时请求**快速失败**为 **`503 bulkhead_rejected`**，不做
   无界排队；许可在 `finally`（含超时/取消）中释放，响应显式关闭，连接归还连接池，
   因此后续波次行为与首轮完全一致。

失败都是**明确、可区分、可观测**的结果，而不是挂起或静默降级：

| HTTP | outcome | 触发条件 |
| --- | --- | --- |
| 200 | `success` | 下游 2xx |
| 429 | `rate_limited` | 超出准入容量（未调用下游） |
| 503 | `bulkhead_rejected` | 在途下游调用已满 |
| 504 | `timeout` | 下游超过截止时间 |
| 502 | `internal_error` | 下游连接错误 / 非 2xx |
| 500 | `internal_error` | 服务端未预期异常 |

## 三、指标含义（Observability）

每个请求按上述五类之一记录**端到端时延**，并周期性（`METRICS_FLUSH_INTERVAL_SEC`）
把本进程增量以「加法 upsert」写入共享库，多 worker 自动聚合、无重复计数。

`GET /metrics` 对每个 outcome 返回：

* `count`：次数；并含顶层 `total`。
* `latency_mean_sec` / `latency_stddev_sec`：由 sum / sum-of-squares 计算。
* `latency_min_sec` / `latency_max_sec`。
* `latency_p50_sec` / `latency_p90_sec` / `latency_p99_sec`：由**对数等宽直方图**
  （1ms~60s，桶在各进程间可相加）估计，无需逐请求存行。

**冷启动/预热语义一致**：计数从 0 开始；任何时刻快照都包含全部五个 outcome 且字段
齐全（无观测时为 0），拒绝/超时与成功走同一条埋点路径，因此冷启动与稳态的数据形状
完全相同。

## 四、本地验证

### 手动验证

```bash
uv run uvicorn main:app --port 8000 &

# 成功
curl -s localhost:8000/data
# 超时（下游延迟 2s > 默认 1s 超时）→ 504，约 1s 返回
curl -s -w '\n%{http_code} %{time_total}s\n' \
  'localhost:8000/data?target=http%3A%2F%2Flocalhost%3A8000%2Finternal%2Fdownstream%3Fdelay%3D2'
# 指标
curl -s localhost:8000/metrics
```

多进程验证限流一致性：

```bash
RATE_LIMIT_CAPACITY=12 RATE_LIMIT_REFILL_PER_SEC=0.01 \
uv run uvicorn main:app --workers 2 --port 8000
# 24 个并发请求 → 恰好 12 个 200、12 个 429
```

### 自动化测试

```bash
uv run pytest -s            # -s 查看每次洪泛打印的并发规模/时延分布/拒绝计数
```

测试覆盖三类场景（均对真实 uvicorn 子进程发真实 HTTP）：

* **高并发 + 慢依赖** `test_hardening_throughput.py`：64 并发打 2s 慢依赖。
  改造前同步阻塞路径受线程池/依赖时延拖累；加固路径在超时阈值内全部决策，
  失败精确分成 32×504（占满舱壁后超时）与 32×503（快速拒绝），并断言
  **端到端吞吐提升 ≥ 3×**（实测约 6×），重复波次分布一致以证明资源被回收。
* **限流** `test_rate_limit.py`：突发精确接纳/拒绝、持续低速零误伤、令牌恢复、
  以及 **2 worker 共享桶不超发**。
* **观测** `test_metrics_and_observability.py`：冷启动零值形状稳定、五种 outcome
  可区分、p50/p90/p99 时延统计、跨进程计数聚合。

每个洪泛用例都会打印，例如：

```
[hardened/slow-dep] total_requests=64 wall_sec=0.8 throughput=83.6 req/s
  status_counts={503: 32, 504: 32}
  outcomes={'bulkhead_rejected': 32, 'timeout': 32}
  latency sec: min=0.10 p50=0.66 p90=0.74 p99=0.74 max=0.75 mean=0.41
```

## 五、配置项（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `RATE_LIMIT_CAPACITY` | 20 | 令牌桶容量（最大突发） |
| `RATE_LIMIT_REFILL_PER_SEC` | 10 | 每秒补充令牌数（持续速率） |
| `DOWNSTREAM_TIMEOUT_SEC` | 1.0 | 下游调用硬超时（秒） |
| `DOWNSTREAM_MAX_INFLIGHT` | 20 | 在途下游调用上限（舱壁/连接池） |
| `SERVICE_DB_PATH` | `.state/service.db` | 跨进程共享 SQLite 路径 |
| `METRICS_FLUSH_INTERVAL_SEC` | 1.0 | 指标增量落库周期（秒） |
