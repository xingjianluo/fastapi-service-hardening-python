# FastAPI Service Hardening

在最小 FastAPI 骨架（`GET / -> {"Hello": "World"}`）的基础上，叠加**并发准入控制、
下游隔离与超时、全链路观测**三层能力，并保持原有接口与响应完全向后兼容。

## 目录结构

```
main.py                # 仅暴露 app = create_app()
app/config.py          # 全部可调参数（环境变量覆盖，含按目标覆盖项）
app/state.py           # 共享 SQLite（WAL）连接与表结构
app/rate_limit.py      # 跨进程令牌桶（BEGIN IMMEDIATE 串行化）
app/bulkhead.py        # 跨进程两级舱壁：全局总闸 + 按逻辑端点隔离（租约许可）
app/downstream.py      # 硬超时 + 稳定失败分类的下游 HTTP 调用（httpx 连接池）
app/metrics.py         # 结果分类计数、时延统计、对数分桶 p50/p90/p99、按目标拆分
app/outcomes.py        # 八类结果枚举
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
| `GET /data?target=<url>` | 加固路径：限流 → 两级舱壁 → 带超时与失败分类的下游调用 |
| `GET /upstream-legacy?target=<url>` | **改造前**对照路径：同步阻塞、无超时、无隔离（用于量化改善） |
| `GET /internal/downstream?delay=&fail=` | 内置可控慢/故障依赖，便于本地验证 |
| `GET /internal/deps/<name>?delay=&fail=&status_code=` | 路径化内置依赖：同一 host:port 上的多个逻辑端点，验证同源路径隔离 |
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

### 2.1 怎么区分不同的下游目标

目标身份是下游 URL 的**逻辑端点**：

```
scheme://host:port/path        （查询串 ?... 与 #fragment 不属于身份）
```

因此：

* **同一域名 + 端口、不同路径 = 不同目标**，各自拥有独立的在途容量与超时，
  互不挤占。例如 `https://api.example.com/v1/orders` 变慢，不会拖累
  `https://api.example.com/v1/inventory`。
* 同一路径仅查询参数不同（`/p?a=1` 与 `/p?a=2`）视为**同一目标**（同一个端点，
  只是参数不同），共享容量与超时。
* scheme 不同（`http` vs `https`）也是不同目标；无法解析的 URL 归到 `unknown`。

### 2.2 两级舱壁 + 逐目标硬超时

对下游调用施加**全局总闸 + 目标级舱壁 + 逐目标硬超时**：

1. **目标级舱壁**：每个逻辑端点有独立的在途上限 `max_inflight`。慢端点只能占用
   自己的份额，份额用完后对该端点的请求**快速失败**为 **`503 target_rejected`**，
   不排队、不挤占同源的其它端点——健康端点（哪怕同 host:port、只是路径不同）的
   成功率与时延不受影响。
2. **全局总闸**：所有目标共享全局在途上限 `DOWNSTREAM_MAX_INFLIGHT`，作为最终
   断路器。全局闸门耗尽时拒绝为 **`503 bulkhead_rejected`**（与目标级拒绝可区分）。
3. **逐目标硬超时**：每个目标有独立的 `timeout_sec`，调用包裹 `asyncio.timeout`，
   超时转成 **`504 timeout`**，时延被钉死在该目标的超时阈值附近。

### 2.3 许可与跨进程一致性

许可不是进程内信号量，而是共享 SQLite 中的 `inflight_permits` 行
（scope = `global` 或 `target:scheme://host:port/path`）。获取在单个
`BEGIN IMMEDIATE` 事务内完成「清理过期租约 → 计数 → 插入」，因此多 worker 下
目标级与全局在途数都**绝不超额**，也不存在按实例切分额度导致的「个别实例耗尽
整体」问题。每个许可带租约（`timeout_sec + 10s`）：

* **正常路径**在 `finally` 中**立即释放**——成功、超时、网络失败、下游报错、
  **以及请求中途被客户端取消**都一样，容量立刻归还；
* 持有者进程崩溃时，租约到期后被惰性回收，容量不会永久泄漏；
* 连续多轮压力下各波次分布一致，不会越跑越差或出现分布漂移。

### 2.4 每个目标的容量与超时如何配置

* 未配置任何目标规则时，目标级默认值回退到全局值，行为与旧版一致（兼容）。
* `TARGET_DEFAULT_MAX_INFLIGHT` / `TARGET_DEFAULT_TIMEOUT_SEC`：所有未显式覆盖
  的目标共用的默认容量/超时。
* `TARGET_OVERRIDES`：JSON 对象，按**键的粒度**覆盖，字段可只给其一。键的查找
  顺序为「精确端点键 → scheme 限定的源站键 → 旧版 `host:port` 键」，并且是
  **逐字段合并**：

```jsonc
{
  // 只收紧这一个路径（最常用：同源不同路径各自隔离）
  "http://127.0.0.1:9001/internal/deps/slow": {"max_inflight": 4, "timeout_sec": 0.5},

  // 源站级：对该 host:port 的所有路径生效（同时兼容新旧两种写法）
  "http://api.example.com": {"max_inflight": 8},
  "127.0.0.1:9002": {"timeout_sec": 0.3}
}
```

  精确端点规则只给 `max_inflight` 时，`timeout_sec` 仍会继承源站级规则或默认值。

### 2.5 容量用尽与各类失败各自的返回含义

失败都是**明确、可区分、可观测**的结果，而不是挂起或静默降级。除 HTTP 状态码
外，每个 `/data` 响应都带机器可读的 `outcome`、`error`、细粒度 `error_kind` 与
归属 `target` 字段：

| HTTP | outcome | error / error_kind | 触发条件 |
| --- | --- | --- | --- |
| 200 | `success` | — | 下游 2xx |
| 429 | `rate_limited` | `rate limit exceeded` | 超出准入容量（未调用下游，不耗令牌） |
| 503 | `bulkhead_rejected` | `bulkhead_rejected` | **全局**在途闸门已满（所有目标的总闸） |
| 503 | `target_rejected` | `target_bulkhead_rejected` | **该目标自身**容量份额耗尽（不影响其它目标） |
| 504 | `timeout` | `downstream_timeout` / `deadline_exceeded`、`transport_timeout` | 下游超过该目标截止时间 |
| 502 | `downstream_error` | `downstream_error` / `http_error_status` | **连得上**下游、它主动返回了非 2xx（下游业务/协议错误），响应含 `downstream_status_code` |
| 502 | `downstream_unreachable` | `downstream_unreachable` / `connection_refused`、`dns_failure`、`network_unreachable`、`connection_reset`、`connect_error`、`protocol_error` | **根本没拿到 HTTP 响应**：端口拒绝、网络不通、域名解析失败、连接被重置/协议中断 |
| 500 | `internal_error` | `internal_error` / `invalid_target_url`、`client_error`、`unexpected_error` | 服务自身或客户端侧未预期异常 |

**关键区分**：「网络断了」与「对方业务报错」虽然都是 502（HTTP 惯例上都是
Bad Gateway），但 `outcome` 与 `error` 分别是 `downstream_unreachable` 与
`downstream_error`，`error_kind` 进一步给出 `connection_refused` /
`dns_failure` / `network_unreachable` / `connection_reset` /
`http_error_status` 等稳定机器码，调用方可直接据此分支处理（重试、熔断、告警
归并各不相同）。网络层原因通过遍历异常 cause 链（httpx → httpcore → socket）
得出，而非匹配易变的错误字符串。两类 502 又都与该目标的超时（504）、容量
拒绝（503）和服务内部异常（500）严格分开。

两类 503 通过 `error`（`bulkhead_rejected` vs `target_bulkhead_rejected`）
区分；每个 `/data` 响应还带 `target` 标明归属的逻辑端点。

## 三、指标含义（Observability）

每个请求按上述八类结果之一记录**端到端时延**，并周期性（`METRICS_FLUSH_INTERVAL_SEC`）
把本进程增量以「加法 upsert」写入共享库，多 worker 自动聚合、无重复计数。

`GET /metrics` 对每个 outcome 返回：

* `count`：次数；并含顶层 `total`。
* `latency_mean_sec` / `latency_stddev_sec`：由 sum / sum-of-squares 计算。
* `latency_min_sec` / `latency_max_sec`。
* `latency_p50_sec` / `latency_p90_sec` / `latency_p99_sec`：由**对数等宽直方图**
  （1ms~60s，桶在各进程间可相加）估计，无需逐请求存行。

另有顶层 `targets` 字段，按**逻辑端点**（`scheme://host:port/path`）拆分每个目标
各自的成功、超时、目标级/全局被拒、`downstream_error`、`downstream_unreachable`、
内部错误计数与 `latency_mean/min/max`。每个出现过的目标都**零值补齐全部八个
outcome**，因此多实例部署时计数严格归到自己名下、不串台、语义一致；全局总量
（顶层各 outcome 与 `total`）始终是最后的兜底上限。可直接从快照看出每个目标的
并发规模（成功/在途超时/拒绝配比）、时延分布与各类拒绝计数。

**冷启动/预热语义一致**：计数从 0 开始；任何时刻快照都包含全部八个 outcome 且字段
齐全（无观测时为 0），各类失败与成功走同一条埋点路径，因此冷启动与稳态的数据形状
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

# 下游主动报错（连得上、对方返回 500）→ 502 / outcome=downstream_error
curl -s 'localhost:8000/data?target=http%3A%2F%2Flocalhost%3A8000%2Finternal%2Fdeps%2Ffast%3Ffail%3Dtrue'
# 网络连不上（本机 1 端口拒绝连接）→ 502 / outcome=downstream_unreachable，毫秒级返回
curl -s 'localhost:8000/data?target=http%3A%2F%2F127.0.0.1%3A1%2Fdead'
# 域名解析不了 → 502 downstream_unreachable / error_kind=dns_failure
curl -s 'localhost:8000/data?target=http%3A%2F%2Fnonexistent-target.invalid%2Fx'

# 指标
curl -s localhost:8000/metrics
```

多进程验证限流一致性：

```bash
RATE_LIMIT_CAPACITY=12 RATE_LIMIT_REFILL_PER_SEC=0.01 \
uv run uvicorn main:app --workers 2 --port 8000
# 24 个并发请求 → 恰好 12 个 200、12 个 429
```

同源不同路径隔离手动复现。目标身份含路径，因此同一台服务器上的
`/internal/deps/slow` 与 `/internal/deps/fast` 就是**两个目标**，尽管域名、端口
完全一致：把 slow 路径配置为容量 2、超时 0.5s，fast 路径用默认容量：

```bash
# 键必须是完整逻辑端点（先确认本服务端口，示例为 8000）
TARGET_OVERRIDES='{"http://localhost:8000/internal/deps/slow": {"max_inflight": 2, "timeout_sec": 0.5}}' \
DOWNSTREAM_MAX_INFLIGHT=20 RATE_LIMIT_CAPACITY=100000 RATE_LIMIT_REFILL_PER_SEC=100000 \
uv run uvicorn main:app --host 127.0.0.1 --port 8000 &

# 慢路径（delay=2s > 0.5s 超时）并发 8 个压在后台
seq 8 | xargs -P8 -I{} curl -s -o /dev/null -w 'slow %{http_code}\n' \
  'localhost:8000/data?target=http%3A%2F%2Flocalhost%3A8000%2Finternal%2Fdeps%2Fslow%3Fdelay%3D2' | sort | uniq -c &
sleep 0.2
# 同时打同源的快路径：全部 200，不被慢路径拖累
seq 8 | xargs -P8 -I{} curl -s -o /dev/null -w 'fast %{http_code}\n' \
  'localhost:8000/data?target=http%3A%2F%2Flocalhost%3A8000%2Finternal%2Fdeps%2Ffast%3Fdelay%3D0.02' | sort | uniq -c
wait
# 输出：fast 8×200；slow 2×504（占满 2 个许可后超时）+ 6×503 target_rejected

# 每个目标各自的并发结果、时延分布与各类拒绝计数
curl -s localhost:8000/metrics | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["targets"], indent=2))'
```

> 当然，用不同 host 区分仍然成立：`localhost:8000` 与 `127.0.0.1:8000` 也是两个
> 目标。注意 `TARGET_OVERRIDES` 的键与 `target` URL 的端点身份**逐字符对应**；
> 也可只写源站（`http://host:port` 或旧版 `host:port`）对整源站生效。

### 自动化测试

```bash
uv run pytest -s            # -s 查看每次洪泛打印的并发规模/时延分布/拒绝计数
```

测试覆盖以下场景（真实 HTTP 用例均对真实 uvicorn 子进程发起）：

* **高并发 + 慢依赖** `test_hardening_throughput.py`：64 并发打 2s 慢依赖。
  改造前同步阻塞路径受线程池/依赖时延拖累；加固路径在超时阈值内全部决策，
  失败精确分成 32×504（占满舱壁后超时）与 32×503（快速拒绝），并断言
  **端到端吞吐提升 ≥ 3×**（实测约 6×），重复波次分布一致以证明资源被回收。
* **限流** `test_rate_limit.py`：突发精确接纳/拒绝、持续低速零误伤、令牌恢复、
  以及 **2 worker 共享桶不超发**。
* **观测** `test_metrics_and_observability.py`：冷启动零值形状稳定、八种 outcome
  可区分、p50/p90/p99 时延统计、跨进程计数聚合；并分别断言「主动报错」与
  「连不上」落在不同 outcome 与不同目标名下。
* **目标级隔离** `test_target_isolation.py`：两个独立源站（两个依赖子进程，
  一慢 2s 一快 20ms）并发施压。验证慢目标只占自己的 4 个许可（4×504 + 28×
  `target_rejected`），快目标 32/32 全部成功且 p99 远低于慢目标超时；目标级拒绝与
  超时/下游主动报错/连接失败/内部异常逐一可区分；连续 3 波分布无漂移（许可回收）；
  2 worker 下目标级与全局限额共同精确生效；`/metrics` 的 `targets` 拆分与之一致。
* **同源路径隔离与失败分类（新增）** `test_endpoint_isolation.py`，全部可离线稳定
  复现、不依赖线程调度时序、不访问公网：
  * **一慢一快两个同源端点**（同一 host:port 的 `/internal/deps/slow` 与
    `/internal/deps/fast`）并发施压：慢路径 4×504 + 28×`target_rejected`，快路径
    32/32 成功且 p99 < 慢路径超时，证明路径不同即容量/超时独立、互不挤占；2 worker
    下同样成立；
  * **网络连不上 vs 下游主动报错可区分**：关闭的本地端口 → 502
    `downstream_unreachable/connection_refused`（毫秒级），活依赖返回 500 → 502
    `downstream_error/http_error_status`，并与 504 超时、503 容量拒绝、500 内部
    异常逐一分开，计数在 `/metrics` 各归其目标；
  * **连续 4 波容量正常回收**：每波后直接只读核对共享库 `inflight_permits`，目标级
    在途数必须归零，各波 504/503/200 分布无漂移；
  * **中途取消即释放**：占满许可后客户端取消所有在途请求，许可在远小于依赖时延/租约
    周期的时间内归还，随后一波仍能拿到完整份额（不会少一个）。
* **分类器单元（新增，纯离线）** `test_downstream_classifier_unit.py`：构造
  httpx/httpcore/socket 同款异常（含 cause 链），断言连接拒绝、DNS 失败
  (`dns_failure`)、网络不可达、连接重置、协议中断归 `downstream_unreachable`；
  各类超时（含同时是 ConnectError 的 ConnectTimeout）归 `timeout`；非法 URL 等归
  内部错误；三类集合两两不相交。
* **舱壁单元** `test_bulkhead_unit.py`：逻辑端点身份解析（含同源不同路径、查询串
  排除）、精确端点/源站/旧键的逐字段覆盖优先级、目标间隔离（含同源路径隔离）、
  全局总闸、许可回收复用、并发不超发、崩溃持有者租约到期回收。

每个洪泛用例都会打印每个目标的并发规模、时延分布（min/p50/p90/p99/max/mean）和
各类拒绝/失败计数，例如：

```
[ep-isolation/slow] total_requests=32 wall_sec=0.41 throughput=78.0 req/s
  status_counts={503: 28, 504: 4}
  outcomes={'target_rejected': 28, 'timeout': 4}
  latency sec: min=0.40 p50=0.41 p90=0.42 p99=0.42 max=0.42 mean=0.41
[ep-isolation/fast] total_requests=32 wall_sec=0.41 throughput=78.0 req/s
  status_counts={200: 32}
  outcomes={'success': 32}
  latency sec: min=0.02 p50=0.02 ...
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
| `TARGET_OVERRIDES` | 空 | JSON：按 `{"<键>": {"max_inflight": N, "timeout_sec": S}}`；键可取完整逻辑端点 `scheme://host:port/path`（最优先）、源站 `scheme://host:port`、或旧版 `host:port`，逐字段合并 |
| `SERVICE_DB_PATH` | `.state/service.db` | 跨进程共享 SQLite 路径 |
| `METRICS_FLUSH_INTERVAL_SEC` | 1.0 | 指标增量落库周期（秒） |

## 六、多进程边界与兼容范围

* **一致语义**：限流令牌桶、目标级/全局在途许可、失败计数全部落在共享 SQLite，
  请求落到哪个 worker 都不改变判定结果；许可 scope 与指标 `targets` 均以完整逻辑
  端点 `scheme://host:port/path` 为名，多实例部署时每个目标各自的成功/超时/被拒/
  各类失败计数严格归到自己名下、不串台；全局上限始终是最终总闸。
* **租约边界**：许可租约为 `目标超时 + 10s`。worker 崩溃后其许可最多占用一个租约
  周期即被回收；这是崩溃恢复的唯一时间边界，正常路径（含超时、网络失败、下游报错、
  客户端中途取消）在 `finally` 立即释放，不受该时间影响。
* **失败语义边界**：`downstream_error` 仅表示「连上了且拿到非 2xx 响应」；凡是
  没拿到完整 HTTP 响应（拒绝连接 / DNS 失败 / 网络不可达 / 重置 / 协议中断）一律是
  `downstream_unreachable`；两者与 `timeout`、两类 503、500 内部异常互不重叠。
* **兼容范围**：`GET /` 与成功响应结构不变；`bulkhead_rejected`、`target_rejected`、
  `rate_limited`、`timeout` 等既有结果与跨进程聚合行为保持。原「连接失败 / 下游
  非 2xx」合并在 502 `internal_error` 的旧语义，按需求拆分为两个稳定、可区分的
  outcome：`downstream_error` 与 `downstream_unreachable`；目标身份由 `host:port`
  细化为 `scheme://host:port/path`（同源不同路径得以隔离），同时 `TARGET_OVERRIDES`
  保留对旧版 `host:port` 键的支持，旧配置无需改动即可继续工作。

## 七、本地复现与验证（离线）

```bash
uv sync --group dev
uv run pytest -s                      # 全部用例；-s 查看每波并发规模/时延/各类拒绝计数
uv run pytest tests/test_downstream_classifier_unit.py   # 纯离线、毫秒级
uv run pytest tests/test_endpoint_isolation.py           # 同源隔离/两类 502/容量回收
```

* 「连不上」用**关闭的本地端口** `http://127.0.0.1:1/...` 触发，立即、确定性地得到
  `connection_refused`，不依赖公网；DNS 分类由纯离线的分类器单测用 `gaierror` 固定。
* 慢/故障依赖全部由内置端点（`/internal/downstream`、`/internal/deps/<name>`）模拟，
  无需任何外部服务。
* 所有并发断言基于**精确计数**与每波后的许可表归零检查，不依赖线程调度时序，连续
  多轮运行结果稳定、无偶发失败。
