# taskmq-rocketmq（示例插件）

演示「**不改 taskmq 源码**接入自己的后端」（设计：[docs/design/plugins.md](../../docs/design/plugins.md)）。
这里的 `RocketMQTransport` 是**假实现**：用进程内存储模拟一台「按优先级 + 延迟投递」的消息队列。
接入真实阿里云 RocketMQ 时，把 `_FakeMQClient` 换成官方 SDK，其余结构不变。

## 三条接入路径

```bash
# ① 打包安装后自动发现（本包已在 pyproject.toml 声明 entry point）
#    注意：taskmq 还没发布到 PyPI，从本仓库装示例要用 --no-deps
uv pip install --no-deps -e examples/plugin_rocketmq
taskmq --app myapp:app status -Q rocket        # 不需要任何 --plugins
```

```python
app = App(Config(transport="rocketmq://rmq.aliyuncs.com:8080/taskmq?group=workers"))

# ② 私有环境不打包：显式加载
sys.path.insert(0, "examples/plugin_rocketmq")
app = App(Config(transport="rocketmq://..."))
app.load_plugins(["taskmq_rocketmq"])          # 或 TASKMQ_PLUGINS=taskmq_rocketmq

# ③ 自己管生命周期：直接给实例
app = App(Config(transport=RocketMQTransport(endpoints="...", topic="taskmq")))
```

```bash
PYTHONPATH=examples/plugin_rocketmq \
  taskmq --app myapp:app --plugins taskmq_rocketmq worker -Q rocket --once
```

## 插件作者要做的三件事

1. **实现 `Transport` 协议**：只依赖 `taskmq.transport.base` 的数据类型与 `taskmq.protocol.Envelope`
   （不要 import `taskmq.worker.*` 或任何 `_` 开头的内部结构）；
2. **如实声明能力**：`supports_leases` / `supports_workers` 决定框架会不会调用对应方法；
   语义对不齐的地方写进 `limitations`（`taskmq status` 会打印，一致性套件会跳过）；
3. **注册 scheme**：`register_transport("rocketmq", factory)`，factory 拿到 `TransportOptions`
   （`url` / `codec` / `codec_registry` / `max_message_bytes` / `idempotency_ttl` / `config`）。

## 自证语义（强烈建议加进你的 CI）

```python
from taskmq.testing import transport_conformance

def test_rocketmq_conformance():
    transport_conformance(
        lambda: RocketMQTransport(endpoints="localhost:8080", topic="taskmq-test"),
        supports={"leases": False, "workers": False},
    )
```

本示例在套件里的结果：**12 个场景通过**，`yield` / `named_leases` / `worker_registry` / `job_listing`
四个场景**按声明跳过** —— 能力的差异是显式写出来的，不是靠测试静默失效掩盖的
（`job_listing` 缺失意味着这个后端上不能跑 DAG 工作流，提交时会直接报错，而不是静默不推进）。
