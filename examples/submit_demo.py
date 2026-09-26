
import time
from examples.demo_app import app

handles = [app.submit("demo.slow", (0.4,)) for _ in range(5)]
urgent = app.submit("demo.add", (100, 1), priority=9)
normal = [app.submit("demo.add", (i, i)) for i in range(20)]
bad = app.submit("demo.flaky", (False,))
print("submitted:", len(handles) + 22, "urgent job:", urgent.id)

deadline = time.time() + 30
while time.time() < deadline:
    stats = app.transport.queue_stats(["demo"])[0]
    if stats.pending == 0 and stats.inflight == 0:
        break
    time.sleep(0.1)
stats = app.transport.queue_stats(["demo"])[0]
print(f"queue: pending={stats.pending} inflight={stats.inflight} dead={stats.dead}")
print("urgent result:", urgent.get(timeout=5))
print("normal ok:", all(h.successful() for h in normal))
print("flaky state:", bad.state)
