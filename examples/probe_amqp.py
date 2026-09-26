"""手工探测 AMQP transport 的脚本（先 `make mq-up` 起测试用的 RabbitMQ）。

    python examples/probe_amqp.py
"""
import tempfile, time, uuid
from taskmq import Envelope
from taskmq.transport.factory import build_transport

tmp = tempfile.mkdtemp()
prefix = f"smoke{uuid.uuid4().hex[:6]}."
url = f"amqp://taskmq:taskmq@127.0.0.1:55672/%2F?state=sqlite:///{tmp}/state.db&prefix={prefix}"
t = build_transport(url)
for task, prio in (("low", 0), ("mid", 1), ("high", 5)):
    t.enqueue(Envelope(task=task, priority=prio, queue="q"), queue="q")
time.sleep(0.2)
granted = t.reserve(["q"], worker_id="w", lease=30, limit=5)
print("同队列优先级:", [(d.envelope.task, d.priority) for d in granted])
print("stats:", t.queue_stats(["q"]))
t.ack(granted[0]); t.ack(granted[0])
t.defer(granted[1], delay=0.5)
print("延迟期间 reserve:", len(t.reserve(["q"], worker_id="w", lease=30, limit=5)))
time.sleep(0.9)
back = t.reserve(["q"], worker_id="w", lease=30, limit=5)
print("延迟回来:", [(d.envelope.task, d.deliveries) for d in back])
t.dead_letter(back[0], "boom")
dead = t.dead_letters()
print("DLQ:", [(d.task, d.reason) for d in dead])
print("replay:", t.replay_dead(dead[0].message_id, priority=7))
got = t.reserve(["q"], worker_id="w", lease=0.01, limit=1)
time.sleep(0.05)
print("reap:", t.reap_expired_leases(), "| 拿回:", [d.envelope.task for d in t.reserve(["q"], worker_id="w", lease=5, limit=1)])
t._drop_queues("q"); t.close()
