# FastAPI Service Hardening

在最小 FastAPI 骨架（`GET / -> {"Hello": "World"}`）的基础上，叠加**并发准入控制、
下游隔离与超时、全链路观测**三层能力，并保持原有接口与响应完全向后兼容。

## 目录结构

```
main.py                # 仅暴露 app = create_app()
app/config.py          # 全部可调参数（环境变量覆盖，含按目标覆盖项）
app/state.py           # 共享 SQLite（WAL）连接与表结构
app/rate_limit.py      # 跨进程令牌桶（BEGIN IMMEDIATE 串行化）
app/bulkhead.py        # 跨进程两级舱壁：全局总闸 + 按下游目标隔离（租约许可）
app/downstream.py      # 带逐调用硬超时的下游 HTTP 调用（httpx 连接池）
app/metrics.py         # 结果分类计数、时延统计、对数分桶 p50/p90/p99、按目标拆分
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

对下游调用施加**两级舱壁 + 逐目标硬超时**：

1. **目标级舱壁**：每个下游目标（以 URL 的 `host:port` 为身份）有独立的在途上限
   `max_inflight`。慢目标只能占用自己的份额，份额用完后对该目标的请求**快速失败**为
   **`503 target_rejected`**，不会排队、更不会挤占其它目标的容量——健康目标的成功率和
   时延不受影响。
2. **全局总闸**：所有目标共享全局在途上限 `DOWNSTREAM_MAX_INFLIGHT`，作为最终断路器。
   全局闸门耗尽时拒绝为 **`503 bulkhead_rejected`**（与目标级拒绝可区分）。
3. **逐目标硬超时**：每个目标有独立的 `timeout_sec`，调用包裹 `asyncio.timeout`，
   超时转成 **`504 downstream_timeout`**，时延被钉死在该目标的超时阈值附近。

**跨进程一致性**：许可不是进程内信号量，而是共享 SQLite 中的 `inflight_permits` 行
（scope = `global` 或 `target:<host:port>`）。获取在单个 `BEGIN IMMEDIATE` 事务内完成
「清理过期租约 → 计数 → 插入」，因此多 worker 下目标级与全局在途数都**绝不超额**，
也不存在按实例切分额度导致的「个别实例耗尽整体」问题。每个许可带租约
（`timeout_sec + 10s`）：正常路径在 `finally`（含超时/取消/异常）中立即释放；持有者
进程崩溃时，租约到期后被惰性回收，容量不会永久泄漏。连续多轮压力下各波次分布一致。

**目标配置语义**（运行时环境变量即可调整）：

* 未配置任何目标规则时，目标级默认值回退到全局值，行为与上一版完全一致（兼容）。
* `TARGET_DEFAULT_MAX_INFLIGHT` / `TARGET_DEFAULT_TIMEOUT_SEC`：所有未显式覆盖的
  目标共用的默认容量/超时。
* `TARGET_OVERRIDES`：JSON 对象，按 `host:port` 覆盖，字段可只给其一，例如
  `TARGET_OVERRIDES='{"127.0.0.1:9001": {"max_inflight": 4, "timeout_sec": 0.5}}'`。

失败都是**明确、可区分、可观测**的结果，而不是挂起或静默降级：

| HTTP | outcome | 触发条件 |
| --- | --- | --- |
| 200 | `success` | 下游 2xx |
| 429 | `rate_limited` | 超出准入容量（未调用下游） |
| 503 | `bulkhead_rejected` | **全局**在途闸门已满（所有目标共享的总闸） |
| 503 | `target_rejected` | **该目标自身**容量份额耗尽（不影响其它目标） |
| 504 | `timeout` | 下游超过该目标的截止时间 |
| 502 | `internal_error` | 下游连接错误 / 非 2xx |
| 500 | `internal_error` | 服务端未预期异常 |

两类 503 通过响应体的 `error`（`bulkhead_rejected` vs `target_bulkhead_rejected`）
与 `outcome` 字段区分；每个 `/data` 响应还带 `target` 字段标明归属的下游目标。

## 三、指标含义（Observability）

每个请求按上述五类之一记录**端到端时延**，并周期性（`METRICS_FLUSH_INTERVAL_SEC`）
把本进程增量以「加法 upsert」写入共享库，多 worker 自动聚合、无重复计数。

`GET /metrics` 对每个 outcome 返回：

* `count`：次数；并含顶层 `total`。
* `latency_mean_sec` / `latency_stddev_sec`：由 sum / sum-of-squares 计算。
* `latency_min_sec` / `latency_max_sec`。
* `latency_p50_sec` / `latency_p90_sec` / `latency_p99_sec`：由**对数等宽直方图**
  （1ms~60s，桶在各进程间可相加）估计，无需逐请求存行。

另有顶层 `targets` 字段，按下游目标（`host:port`）拆分每个目标的 outcome 计数与
`latency_mean/min/max`，可直接看出每个目标的并发规模、时延分布与拒绝计数。

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

目标级隔离手动复现。目标身份是 URL 的 `host:port`，因此同一台服务器的
`localhost:8000` 与 `127.0.0.1:8000` 就是**两个目标**：把 `localhost` 当慢目标
（容量 2、超时 0.5s），`127.0.0.1` 当健康目标（用默认容量）：

```bash
TARGET_OVERRIDES='{"localhost:8000": {"max_inflight": 2, "timeout_sec": 0.5}}' \
DOWNSTREAM_MAX_INFLIGHT=20 RATE_LIMIT_CAPACITY=100000 RATE_LIMIT_REFILL_PER_SEC=100000 \
uv run uvicorn main:app --port 8000 &

# 慢目标（delay=2s > 0.5s 超时）并发 8 个，压在后台
seq 8 | xargs -P8 -I{} curl -s -o /dev/null -w 'slow %{http_code}\n' \
  'localhost:8000/data?target=http%3A%2F%2Flocalhost%3A8000%2Finternal%2Fdownstream%3Fdelay%3D2' | sort | uniq -c &
sleep 0.2
# 同时打健康目标：全部 200，不被慢目标拖累
seq 8 | xargs -P8 -I{} curl -s -o /dev/null -w 'fast %{http_code}\n' \
  'localhost:8000/data?target=http%3A%2F%2F127.0.0.1%3A8000%2Finternal%2Fdownstream%3Fdelay%3D0.02' | sort | uniq -c
wait
# 输出：fast 8×200；slow 2×504（占满 2 个许可后超时）+ 6×503 target_rejected

# 每个目标的并发结果、时延分布与拒绝计数
curl -s localhost:8000/metrics | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["targets"], indent=2))'
```

注意：`TARGET_OVERRIDES` 的键必须与 `target` URL 的 authority **逐字符一致**
（`localhost:8000` 与 `127.0.0.1:8000` 是两个不同目标）。

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
* **目标级隔离** `test_target_isolation.py`：两个独立下游目标（两个依赖子进程，
  一慢 2s 一快 20ms）并发施压。验证慢目标只占自己的 4 个许可（4×504 + 28×
  `target_rejected`），快目标 32/32 全部成功且 p99 远低于慢目标超时；目标级拒绝与
  超时/下游错误/连接失败/内部异常逐一可区分；连续 3 波分布无漂移（许可回收）；
  2 worker 下目标级与全局限额共同精确生效；`/metrics` 的 `targets` 拆分与之一致。
* **舱壁单元** `test_bulkhead_unit.py`：目标身份解析、覆盖项优先级、目标间隔离、
  全局总闸、许可回收复用、并发不超发、崩溃持有者租约到期回收。

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
| `DOWNSTREAM_TIMEOUT_SEC` | 1.0 | 下游调用硬超时（秒），兼作目标级默认超时 |
| `DOWNSTREAM_MAX_INFLIGHT` | 20 | 全局在途下游调用上限（跨进程总闸/连接池） |
| `TARGET_DEFAULT_MAX_INFLIGHT` | 同全局值 | 未覆盖目标的默认在途上限 |
| `TARGET_DEFAULT_TIMEOUT_SEC` | 同全局值 | 未覆盖目标的默认超时（秒） |
| `TARGET_OVERRIDES` | 空 | JSON：`{"host:port": {"max_inflight": N, "timeout_sec": S}}` |
| `SERVICE_DB_PATH` | `.state/service.db` | 跨进程共享 SQLite 路径 |
| `METRICS_FLUSH_INTERVAL_SEC` | 1.0 | 指标增量落库周期（秒） |

## 六、多进程边界与兼容范围

* **一致语义**：限流令牌桶、目标级/全局在途许可、失败计数全部落在共享 SQLite，
  请求落到哪个 worker 都不改变判定结果；全局上限始终是最终总闸。
* **租约边界**：许可租约为 `目标超时 + 10s`。worker 崩溃后其许可最多占用一个租约
  周期即被回收；这是崩溃恢复的唯一时间边界，正常路径不受影响。
* **兼容范围**：`GET /` 与成功响应结构不变；原有五类 outcome、状态码与跨进程聚合
  行为保持；新增 `target_rejected` outcome、`targets` 指标拆分与响应中的 `target`
  字段均为纯增量。未配置任何 `TARGET_*` 变量时，目标级默认值回退到全局值，行为与
  上一版逐项一致（既有测试全部原样通过）。
