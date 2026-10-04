# FastAPI Service Hardening

在最小 FastAPI 骨架（`GET / -> {"Hello": "World"}`）的基础上，叠加**并发准入控制、
按下游目标划分的隔离与超时、全链路观测**三层能力，并保持原有接口与响应完全向后兼容。

## 目录结构

```
main.py                # 仅暴露 app = create_app()
app/config.py          # 全部可调参数（环境变量覆盖）
app/state.py           # 共享 SQLite（WAL）连接与表结构
app/rate_limit.py      # 跨进程令牌桶（BEGIN IMMEDIATE 串行化）
app/permits.py         # 跨进程在途许可（全局 + 目标级容量，带 TTL 回收）
app/targets.py         # 下游目标解析：按 URL 前缀匹配容量/超时策略
app/downstream.py      # 目标级舱壁 + 全局总闸 + 目标级超时
app/metrics.py         # 结果分类计数、时延统计与对数分桶 p50/p90/p99
app/outcomes.py        # 六类结果枚举
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
| `GET /internal/bulkheads` | 各作用域（global + 每个目标）当前在途许可数 |
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

隔离按**下游目标**划分：每个目标拥有独立的容量与超时边界，慢目标只能占用自己的
那份容量，不会拖垮其它目标；全局上限作为最终总闸保留。三层保护：

1. **目标级舱壁**：按 URL 前缀把调用路由到命名目标（`DOWNSTREAM_TARGETS_JSON`），
   每个目标有独立的 `max_inflight`。目标容量用完时，对该目标的请求**快速失败**为
   **`503 target_bulkhead_rejected`**（响应带 `target` 字段），其它目标完全不受影响。
   未匹配任何配置目标的调用归入保留目标 `default`，其容量即全局预算，不重复计数。
2. **全局总闸**：`DOWNSTREAM_MAX_INFLIGHT` 是所有目标共享的在途上限。目标各自
   容量之和可以超过全局值，但全局在途绝不超限；全局耗尽时请求失败为
   **`503 bulkhead_rejected`**（与目标级拒绝可区分）。
3. **目标级超时**：每个调用按所属目标的 `timeout_sec` 包裹 `asyncio.timeout`，
   超时转成 **`504 downstream_timeout`**，时延被钉死在该目标的超时阈值附近。

**跨进程一致性**：目标级与全局容量都是共享 SQLite 中的在途许可（`inflight_permit`
表，一行一个许可），获取在 `BEGIN IMMEDIATE` 事务内完成「清理过期许可 → 计数 →
插入」，与令牌桶同一套串行化机制——因此请求落到不同 worker 也不会超额，个别实例
无法耗尽整体额度。许可带 TTL（超时 + 10s 余量），持有者崩溃时许可到期被回收；
正常路径上许可在 `finally`（成功/失败/超时/取消一致）中释放，连接归还连接池，
因此连续多轮压力下分布稳定、不漂移。

失败都是**明确、可区分、可观测**的结果，而不是挂起或静默降级：

| HTTP | outcome | error 字段 | 触发条件 |
| --- | --- | --- | --- |
| 200 | `success` | — | 下游 2xx |
| 429 | `rate_limited` | `rate limit exceeded` | 超出准入容量（未调用下游） |
| 503 | `target_bulkhead_rejected` | `target_bulkhead_rejected` | 该目标自身容量耗尽 |
| 503 | `bulkhead_rejected` | `bulkhead_rejected` | 全局在途预算耗尽 |
| 504 | `timeout` | `downstream_timeout` | 该目标超过自己的截止时间 |
| 502 | `internal_error` | `downstream_error` | 下游主动返回非 2xx |
| 502 | `internal_error` | `downstream_connect_error` | 网络/连接失败（对端不可达等） |
| 500 | `internal_error` | `internal_error` | 服务端未预期异常 |

## 三、指标含义（Observability）

每个请求按上述六类 outcome 之一记录**端到端时延**，并周期性（`METRICS_FLUSH_INTERVAL_SEC`）
把本进程增量以「加法 upsert」写入共享库，多 worker 自动聚合、无重复计数。

`GET /metrics` 对每个 outcome 返回：

* `count`：次数；并含顶层 `total`。
* `latency_mean_sec` / `latency_stddev_sec`：由 sum / sum-of-squares 计算。
* `latency_min_sec` / `latency_max_sec`。
* `latency_p50_sec` / `latency_p90_sec` / `latency_p99_sec`：由**对数等宽直方图**
  （1ms~60s，桶在各进程间可相加）估计，无需逐请求存行。

**冷启动/预热语义一致**：计数从 0 开始；任何时刻快照都包含全部六个 outcome 且字段
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

目标级隔离本地复现（一慢一快两个目标，慢的不拖垮快的）：

```bash
DOWNSTREAM_MAX_INFLIGHT=64 DOWNSTREAM_TARGETS_JSON='[
  {"name":"slow","match":"/internal/downstream?delay=2","max_inflight":4,"timeout_sec":0.4},
  {"name":"fast","match":"/internal/downstream?delay=0","max_inflight":16,"timeout_sec":1.0}
]' uv run uvicorn main:app --port 8000 --workers 2

# 终端 A：24 个并发打慢目标 → 约 4 个 504 + 20 个 503 target_bulkhead_rejected
seq 24 | xargs -P24 -I{} curl -s -o /dev/null -w '%{http_code}\n' \
  'localhost:8000/data?target=http%3A%2F%2Flocalhost%3A8000%2Finternal%2Fdownstream%3Fdelay%3D2'
# 同时终端 B：快目标照常 200，时延不受慢目标影响
curl -s 'localhost:8000/data?target=http%3A%2F%2Flocalhost%3A8000%2Finternal%2Fdownstream%3Fdelay%3D0.02'
# 观察各作用域在途许可（洪泛结束后应全部归零）
curl -s localhost:8000/internal/bulkheads
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
* **观测** `test_metrics_and_observability.py`：冷启动零值形状稳定、六种 outcome
  可区分、p50/p90/p99 时延统计、跨进程计数聚合。
* **目标级隔离** `test_target_isolation.py`：一慢（容量 4、超时 0.4s）一快
  （容量 16）两个目标并发洪泛——慢目标饱和只产生自己的
  `target_bulkhead_rejected`，快目标成功率/时延不变；目标级拒绝与超时、下游
  主动错误、连接失败、全局总闸逐一可区分；连续 3 波分布稳定且
  `/internal/bulkheads` 每波归零（资源回收）；2 worker 下目标容量不超额。

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
| `DOWNSTREAM_TIMEOUT_SEC` | 1.0 | 下游调用硬超时（秒，未匹配目标的默认值） |
| `DOWNSTREAM_MAX_INFLIGHT` | 20 | 全局在途下游调用上限（跨进程总闸/连接池，也是 `default` 目标容量） |
| `DOWNSTREAM_TARGETS_JSON` | 空 | 目标级策略 JSON 列表：`[{"name","match","max_inflight","timeout_sec"}]`，`match` 为 URL 或路径前缀，最长前缀优先 |
| `SERVICE_DB_PATH` | `.state/service.db` | 跨进程共享 SQLite 路径 |
| `METRICS_FLUSH_INTERVAL_SEC` | 1.0 | 指标增量落库周期（秒） |

## 六、多进程边界与兼容范围

* **多进程一致性**：令牌桶、目标级/全局在途许可、指标计数全部存于共享 SQLite
  （WAL），容量与计数语义与单进程一致；请求落到任一 worker 都不会改变总量语义。
* **兼容范围**：`GET /`、成功响应结构（新增 `target` 字段，纯增量）、既有五类
  outcome 与 429/503/504/502/500 状态码语义、指标快照字段与跨进程汇总行为均保持
  不变；仅**新增** `target_bulkhead_rejected` outcome、`downstream_connect_error`
  错误细分与 `/internal/bulkheads` 观测端点。
