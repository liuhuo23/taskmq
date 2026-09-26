# 插件与自注册后端设计（v0.1 草案）

> 目标：**用户不改 taskmq 源码**就能接自己的后端/组件（阿里云 RocketMQ、公司内部 MQ、自研结果存储、自研事件管道）。核心库只提供**注册表 + 发现 + 契约校验**，不认识任何第三方实现。
>
> **状态：v1.0 已落地**（D1–D7 全部按建议确认）。实现纪要见 §10；文档引用 docs/design.md §22。

---

## 1. 目标 / 非目标

**目标**

- 第三方包 `pip install` 后通过 **entry point** 自动发现，用户只改一行 `Config(transport=...)`；
- 私有环境不打包也能用：`app.load_plugins([...])` / `TASKMQ_PLUGINS=` / CLI `--plugins`；
- 插件**只依赖公开契约**（Transport 协议 + 数据类型），核心内部结构可以随便重构；
- 插件必须**如实声明能力**（`supports_leases` / `supports_workers`），框架启动时校验：缺能力要**启动即报错或明确降级**，不留到运行时诡异失败；
- 提供**一致性测试套件**，第三方后端能自证语义与内建 transport 一致。

**非目标**

- 不做 `sys.path` 目录扫描、不做动态下载安装（供应链风险，明确拒绝）；
- 不做 monkey patch 式隐式挂钩（不可读、难排查）；
- 不做插件沙箱：插件即代码，信任边界 = 已安装的包 + 显式配置。

---

## 2. 现状：插入点在哪（已核对代码）

| 位置 | 现状 | 结论 |
|---|---|---|
| `Config.transport` | 已是 `Any`，校验允许 **Transport 实例**（`hasattr(transport, "enqueue")`，config.py:78） | 最简单路径已成立：**直接传实例**，零基础设施 |
| `App._make_transport()` | app.py:243，按 scheme 硬编码 memory/sqlite/redis，未知 scheme 抛 `ConfigError` | **唯一分派口**，注册表插这里即可 |
| `App.add_sink` / `EventSink` | 已协议化，OTel 就是这么接的 | 事件管道已插件友好 |
| `App.register_codec` | 注册**自定义类型**编解码 | 已有；缺的是**自定义序列化器名**（`_CODEC_FACTORIES` 硬编码，protocol.py:469） |
| `make_pool` | 名字硬编码 solo/threads/asyncio/processes（pool.py:235） | 可插件化，但没有真实需求前不做 |
| 子进程 | 进程池用 `app_spec="module:attr"` 重建 App（execution.py:74） | 插件在子进程也要可加载 → §5 |
| 一致性测试 | 不变量测试散在 `tests/test_transport.py` 等 | 抽成可复用套件 → §6 |

**关键结论**：不需要动现有任何一条代码路径，只在 `_make_transport()` 的「未知 scheme」分支前插入「查注册表」，默认行为完全不变。

---

## 3. 设计

### 3.1 一个薄注册表 `taskmq/plugins.py`

```python
TransportFactory = Callable[[TransportOptions], Transport]
CodecFactory = Callable[[CodecRegistry], Codec]
PoolFactory = Callable[[PoolOptions], Pool]
SinkFactory = Callable[[], EventSink]

@dataclasses.dataclass(frozen=True, slots=True)
class TransportOptions:
    """框架交给插件工厂的全部上下文（插件自己解析 URL 里自己的参数）。"""
    url: str
    codec: Codec                    # 已按 config.serializer + 类型注册表构造好
    codec_registry: CodecRegistry
    max_message_bytes: int
    idempotency_ttl: float
    config: Config                  # 只读快照

def register_transport(scheme: str, factory: TransportFactory, *, override: bool = False) -> None: ...
def register_codec(name: str, factory: CodecFactory, *, override: bool = False) -> None: ...
def register_sink(name: str, factory: SinkFactory, *, override: bool = False) -> None: ...
def register_pool(name: str, factory: PoolFactory, *, override: bool = False) -> None: ...   # 预留

def load_plugins(modules: Iterable[str] | None = None, *, entry_points: bool = True) -> list[str]: ...
def discover(group: str = "taskmq.plugins") -> list[str]: ...   # 只返回模块名，不 import
def known_transports() -> dict[str, str]: ...                   # scheme -> builtin / 插件包名
```

### 3.2 解析顺序（`App._make_transport`）

1. `config.transport` **不是字符串** → 直接返回实例（保留现有语义，给想自己管生命周期的用户）；
2. **内建 scheme**（memory / sqlite / redis）→ 走原路径，**内建优先**；
3. 查注册表 → 命中则 `factory(TransportOptions(...))`；
4. 仍未命中 → 触发一次 **entry point 懒发现**（组 `taskmq.plugins`），再查一次；
5. 仍无 → `ConfigError`，错误信息必须**可操作**：

```
未知 transport scheme：rocketmq://
已注册：memory, sqlite, redis（内建）
想接自己的后端？pip install taskmq-rocketmq，或 app.load_plugins(["mycompany.mq"])
细节见 docs/design/plugins.md
```

> 懒发现很重要：标准部署（memory/sqlite/redis）**不 import 任何第三方包**，启动开销不变。

### 3.3 能力声明与启动校验

```python
class RocketMQTransport(Transport):
    supports_leases = True     # concurrency_key / beat 选主 需要
    supports_workers = True    # status 的 WORKERS 段需要
```

- 已有机制：`Worker._validate_capabilities()` 启动时校验（`concurrency_key` 需要命名租约、async 任务需要 asyncio 池），**启动即 `ConfigError`**；
- 文档写死规则：**声明不支持 = 框架不会调用它**；插件若谎报（声明 True 但方法抛 `NotImplementedError`），一致性套件当场抓（§6）；
- 不支持的项在 CLI 里显式降级显示（例如 `supports_workers=False` 时 `status` 打印「(该 transport 不支持 worker 列表)」）。

### 3.4 URL 约定

`scheme://host:port/path?k=v` 由**插件自定义**语义，框架只做两件事：原样把 `url` 交给插件；在文档里列出**必须遵守**的隔离清单（幂等键、worker 注册表、命名租约都必须落在自己的前缀/命名空间内，避免多环境互踩）。

---

## 4. 示例：阿里云 RocketMQ 插件（不改 taskmq 一行）

### 4.1 插件包

```python
# taskmq_rocketmq/__init__.py
from urllib.parse import parse_qs, urlparse

from taskmq.plugins import TransportOptions, register_transport
from taskmq.transport import (          # 公开契约：这些名字保证向后兼容
    UNSET, DeadLetter, Delivery, JobRecord, JobState, QueueStat, Transport, WorkerInfo,
)
from taskmq.protocol import Envelope, decode_value, encode_value


class RocketMQTransport(Transport):
    supports_leases = True               # 用 RocketMQ 定时消息实现租约
    supports_workers = False             # 不维护 worker 注册表，框架会跳过

    def __init__(self, *, endpoints: str, topic: str, group: str, codec, registry) -> None:
        self._client = _AliyunClient(endpoints, topic, group)   # aliyun SDK 只在插件里依赖
        self._codec, self._registry = codec, registry

    def enqueue(self, env, *, queue=None, delay=0.0, priority=None) -> str:
        routed = env.with_routing(queue or env.queue, env.priority if priority is None else priority)
        self._client.send(self._codec.encode(routed), delay=delay, key=env.key)
        return env.id

    # reserve / ack / nack / dead_letter / extend_lease / set_state / get_state /
    # queue_stats / reap_expired_leases / reap_expired_jobs / acquire_lease / ...


def _factory(options: TransportOptions) -> Transport:
    parsed = urlparse(options.url)
    query = parse_qs(parsed.query)
    return RocketMQTransport(
        endpoints=parsed.netloc or query["endpoints"][0],
        topic=parsed.path.lstrip("/") or "taskmq",
        group=query.get("group", ["taskmq"])[0],
        codec=options.codec,
        registry=options.codec_registry,
    )


register_transport("rocketmq", _factory)
```

```toml
# taskmq_rocketmq/pyproject.toml —— 被 taskmq 自动发现的唯一要求
[project]
name = "taskmq-rocketmq"
dependencies = ["taskmq>=0.1", "aliyun-sdk-rocketmq>=2"]

[project.entry-points."taskmq.plugins"]
rocketmq = "taskmq_rocketmq"
```

### 4.2 用户侧（三种等价写法）

```python
# ① 装了包 → 什么都不用做，scheme 自动可用
app = App(Config(transport="rocketmq://rmq.aliyuncs.com:8080/taskmq?group=workers"))

# ② 私有环境不打包：显式加载模块
app = App(Config(transport="rocketmq://..."))
app.load_plugins(["mycompany.mq_adapters.rocketmq"])

# ③ 完全自己管生命周期：直接给实例（现有能力，无需插件机制）
app = App(Config(transport=RocketMQTransport(...)))
```

```bash
export TASKMQ_PLUGINS=mycompany.mq_adapters.rocketmq       # 环境变量
taskmq --app myapp:app --plugins mycompany.mq worker -Q email
```

### 4.3 自证语义（第三方后端的质量闸门，§6）

```python
from taskmq.testing import transport_conformance

def test_rocketmq_matches_taskmq_semantics():
    transport_conformance(
        lambda: RocketMQTransport(endpoints=..., topic="test", group="t"),
        supports={"leases": True, "workers": False},
    )
```

---

## 5. 进程边界（不设计好，插件在 worker 子进程里会找不到）

1. **entry points**：子进程同样能发现（已安装的包）→ 自动 OK；
2. **显式模块**：随 `ChildTask` 传下去，子进程 `load_plugins(payload.plugins)`——比只靠环境变量更确定；
3. 兜底：`TASKMQ_PLUGINS` 环境变量（文档写明三选一，推荐前两者）。

`Worker` / 池只需把 `app.plugins` 透传进 `ChildTask`（新增字段，有默认值，协议兼容）。

---

## 6. 一致性测试套件（本设计里最有价值的部分）

把内建 transport 已在测的不变量抽成**可复用套件**，第三方后端一行调用即可自证：

```python
def transport_conformance(factory, *, supports=..., clock=None, cleanup=None) -> None:
    """跑一遍 Transport 契约；断言失败带场景名，便于定位。"""
```

| 场景 | 断言 |
|---|---|
| 优先级 | 高优先级先出；同级 FIFO |
| 原子 claim | 8 线程 × 2 连接抢 N 条 → **无重复投递** |
| 幂等 ack | 重复 ack 不报错；迟到 ack 抛 `LeaseLost` |
| 租约回收 | 过期后重投，`deliveries` 递增 |
| `defer` | **不消耗** `deliveries` |
| 让位 | `yields` 独立递增；到 `max_yields` 后返回 False |
| 过期 | 过期消息不投递，job 落 `EXPIRED` |
| 幂等键 | 同 key 第二次不重复入队，返回同一个 job id |
| DLQ | 进 DLQ、可列、可重放（可覆盖优先级） |
| 状态/结果 | `set_state/get_state` 往返，meta 合并 |
| 命名租约（声明支持时） | 抢/续/放；非持有者续租失败 |
| worker 注册表（声明支持时） | 注册/心跳/注销/列表 |
| job 枚举（声明支持时） | `list_jobs` 按前缀/状态过滤，新的在前，`limit` 生效 |
| **能力诚实性** | 声明 `supports_*=False` 的方法必须抛 `TransportError`，不能静默返回假数据 |

> 内建三家 transport 也改成**调用同一个套件**（测试里一行参数化）——保证「给第三方的契约」就是「我们自己也在跑的契约」，避免文档与实现脱节。

---

## 7. 插件 API 的稳定性边界（明确承诺）

**插件可以依赖**（向后兼容；破坏即需要 major/minor + 弃用期）

- `taskmq.transport`：Transport、Delivery、DeadLetter、JobRecord、JobState、QueueStat、UNSET、WorkerInfo、MessageState；
- `taskmq.protocol`：Envelope、Codec、CodecRegistry、encode_value/decode_value、encode_payload/decode_payload；
- `taskmq.plugins`（本设计新增）、`taskmq.errors`、`taskmq.priority`、`taskmq.testing`；
- Transport 的方法签名与语义（§7 契约表）。

**插件不得依赖**（随时可改）：`taskmq.worker.*` 内部、`App._*` 私有属性、任何下划线开头的名字、transport 内部表结构。

---

## 8. 核心改动清单（约 200 行 + 测试，全部新增）

| 文件 | 改动 | 风险 |
|---|---|---|
| `taskmq/plugins.py` | **新增**：注册表 + entry point 发现 + TransportOptions | 低（独立模块） |
| `taskmq/app.py` | 未知 scheme 分支前查注册表；`App.load_plugins()`；`App.plugins`；构造时读 `TASKMQ_PLUGINS` | 低（内建分支不动） |
| `taskmq/protocol.py` | `_CODEC_FACTORIES` 改为「内建 + 可注册」 | 低 |
| `taskmq/cli.py` | 全局 `--plugins module,module`；错误信息带已注册 scheme | 低 |
| `worker/execution.py` + `pool.py` | `ChildTask.plugins` 并透传 | 低（默认空元组） |
| `taskmq/testing.py` | `transport_conformance()`（从现有测试抽取） | 中（要重构现有测试调用它） |
| `examples/plugin_rocketmq/` | **假实现**示例（内存模拟 MQ），证明不改核心也能接 | 低 |

**兼容性**：默认行为零变化（内建优先、无插件不 import 任何东西）；`Config(transport=<实例>)` 不受影响；`ChildTask` 新字段有默认值 → 旧调用方不变。

---

## 9. ✅ 决策点（已确认）

| # | 问题 | 结论 |
|---|---|---|
| D1 | 发现机制 | ✅ **都要**：打包走 entry points（组 `taskmq.plugins`），私有环境走 `TASKMQ_PLUGINS` / `--plugins` / `app.load_plugins()` |
| D2 | 扩展点范围 | ✅ **transport + codec + sink** 已落地；`register_pool` 作为**预留**扩展点（接口在、语义未承诺）；`result=` 独立后端留 Phase 2 |
| D3 | 内建冲突 | ✅ **内建优先**；插件覆盖内建必须显式 `override=True`，否则注册时直接报错 |
| D4 | 子进程传递 | ✅ 写进 `ChildTask.plugins`（`App.plugins` 透传），env 作为兜底 |
| D5 | `transport_conformance` | ✅ 已实现 16 个场景；内建三家 transport 在测试里跑同一套件 |
| D6 | `result=` | ✅ 本轮不做（结果与状态同源） |
| D7 | `limitations` 声明 | ✅ 已实现：`Transport.limitations` + `taskmq status` 打印 + 一致性套件据此**自动跳过**对应场景 |

---

## 10. 落地纪要（v1.0）

**代码**

| 文件 | 内容 |
|---|---|
| `taskmq/plugins.py` | 注册表（transport/codec/sink/pool 四个扩展点）+ entry point 懒发现 + `TransportOptions`/`PoolOptions` + `load_plugin(s)` + `unknown_scheme_message()` |
| `taskmq/app.py` | `_make_transport()` 查注册表（内建优先、`override=True` 可覆盖）+ 未知 scheme 懒发现 + `App.load_plugins()`/`App.plugins`；`include=` 与 `TASKMQ_PLUGINS=` 在 `config.validate()` **之前**加载 |
| `taskmq/protocol.py` | `get_codec` 支持插件注册的序列化器名（内建优先，未知时懒发现） |
| `taskmq/events.py` | `build_sink` 支持插件注册的 sink 名；错误信息列出全部可选名 |
| `taskmq/config.py` | pool 允许插件注册名；`events` 只校验「非空字符串」（名字合法性交给 `build_sink` → **App 构造即失败**） |
| `taskmq/cli.py` | 全局 `--plugins a,b`；`status` 打印 `LIMITATIONS` 段（transport 声明的语义降级） |
| `taskmq/worker/{execution,pool,runner}.py` | `ChildTask.plugins` + 子进程加载（进程池/可强杀两条路径都覆盖） |
| `taskmq/transport/base.py` | `Transport.limitations`（默认空） |
| `taskmq/testing.py` | `transport_conformance()`：16 个场景（含 `capability_honesty` / `job_listing`）；假时钟可注入（`now=`/`advance=`） |
| `examples/plugin_rocketmq/` | 示例插件（假 MQ）：完整 `Transport` 实现 + entry point 声明 + README |

**测试**：`tests/test_plugins.py`（18 例：注册/内建保护/冲突/未知 scheme 报错/显式加载/env/include/entry point 懒发现与 `:attr` 钩子/codec/sink/pool/子进程透传/limitations）、
`tests/test_conformance.py`（8 例：memory + sqlite + redis（Lua 与无 Lua）跑同一套件、声明降级自动跳过、谎报能力被抓、假时钟）、
`tests/test_plugin_example.py`（7 例：示例插件端到端跑任务、`concurrency_key` 启动即报错、`--plugins` 起 worker、未知 scheme 报错可操作）。

**实现中确认的两个细节**

1. **sink 名字的合法性必须在 `App` 构造时判**，不能放在 `Config.validate()`：插件 sink 可能在 validate 之后才注册
   （懒发现），所以 `validate()` 只保证「非空字符串」，`build_sink()` 负责给出「可选值 + 用 register_sink 注册」的可操作报错 —— 依然是 fail fast。
2. **`register()` 要幂等**：模块缓存会让「重复 import」不再产生注册副作用，所以插件应写成
   `if transport_factory("scheme") is None: register_transport(...)`（示例插件就是这么写的）。

**一致性套件立刻抓到的真问题**：示例插件第一版只把消息标成 expired、忘了把 job 置 `EXPIRED`——
`expires` 场景当场失败。这正是「契约必须我们自己也在跑」的价值。

**示例插件跑套件的结果**：**12 个场景通过**，`yield` / `named_leases` / `worker_registry` /
`job_listing` **按声明跳过**（后者意味着该后端上 DAG 工作流不可用，提交时直接报错）。
6. 文档定稿 + README 增补 + `docs/design.md` §22 引用。
