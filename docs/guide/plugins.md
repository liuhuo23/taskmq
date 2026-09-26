# 接入新后端（插件）

**不改 taskmq 源码**就能接自己的 MQ / 存储。三条路径能力等价：

=== "① 打包 + entry point（推荐）"

    ```toml
    # 你的插件包 pyproject.toml
    [project.entry-points."taskmq.plugins"]
    rocketmq = "taskmq_rocketmq"
    ```

    用户只要写 URL，框架会懒发现并加载：

    ```python
    app = App(Config(transport="rocketmq://rmq.example.com:8080/taskmq?group=workers"))
    ```

=== "② 显式加载（私有环境不打包）"

    ```python
    app = App(Config(transport="rocketmq://..."))
    app.load_plugins(["mycompany.mq_adapters.rocketmq"])
    ```

    ```bash
    TASKMQ_PLUGINS=mycompany.mq taskmq --app myapp:app worker -Q email --once
    taskmq --app myapp:app --plugins mycompany.mq status
    ```

=== "③ 自己管生命周期"

    ```python
    app = App(Config(transport=RocketMQTransport(...)))
    ```

## 扩展点

```python
from taskmq.plugins import register_codec, register_pool, register_sink, register_transport

`register_transport("rocketmq", my_factory)      # URL scheme -> Transport
`register_codec("msgpack", my_codec_factory)    # serializer="msgpack"
`register_sink("my-exporter", my_sink_factory)  # events="my-exporter"
`register_pool("uvloop", my_pool_factory)       # pool="uvloop"（预留扩展点）
```

transport 工厂拿到的是 `TransportOptions`（url / codec / registry / 上限 / config），
**只依赖公开契约** `taskmq.transport` / `taskmq.protocol` / `taskmq.plugins`，
别 import `taskmq.worker.*` 或任何下划线内部结构。

## 插件作者要做的三件事

1. **如实声明能力**：

   ```python
   class MyTransport(Transport):
       supports_leases = True       # 影响 concurrency_key / beat 选主
       supports_workers = False     # False → 不注册心跳，status 没有 WORKERS 段
       supports_job_listing = False # False → 提交 DAG 工作流时直接报错（不静默降级）
       limitations = {"global_priority": "MQ 是 per-queue 有序"}
   ```

   声明 `False` 的能力，框架**不会调用**对应方法；声明了却静默成功会被一致性套件抓出来
   （`capability_honesty` 场景）。

2. **在自家 CI 里跑一致性套件自证语义**：

   ```python
   from taskmq.testing import transport_conformance

   def test_my_backend():
       transport_conformance(
           lambda: MyTransport(...),
           supports={"leases": True, "workers": False},
           cleanup=lambda t: t.close(),
       )
   ```

   16 个场景：优先级/FIFO、跨队列优先、原子 claim、ack 语义、租约回收、defer、让位、过期、幂等键、
   DLQ 重放、job 状态、可见性、命名租约、worker 注册表、job 枚举、能力诚实性。

3. **保证线程安全**：同一个实例会被多线程调用（reserve 在主线程、ack 在线程池）。

## 完整示例

[examples/plugin_rocketmq](https://github.com/liuhuo23/taskmq/tree/main/examples/plugin_rocketmq)：
一个假 MQ 实现 + entry point + 自证测试，跑套件 **12 个场景通过、4 个按声明跳过**（包括「不支持 job 枚举
⇒ 这个后端不能跑 DAG 工作流」）。设计与契约见[插件设计](../design/plugins.md)。

## 下一步

- [选择 transport](transports.md)：内建后端的能力矩阵
- [插件与一致性套件设计](../design/plugins.md)：D1–D7 决策记录
