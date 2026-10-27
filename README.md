# kombu-eventbus

Small event bus over RabbitMQ for Python services: a shared JSON envelope, confirmed
publishing and a reconnecting consumer with heartbeats. Wire-compatible with
[`khandroider/laravel-eventbus`](https://github.com/khandroider/laravel-eventbus), so a
Laravel monolith and a Python service can talk through the same exchange.

```
exchange     events (topic, durable)
routing key  {source}.{type}                 e.g. taphr.resume_parse_requested
body         {"version": 1, "id": "<uuid>", "type": "resume_parse_requested", "source": "taphr",
              "occurred_at": "2026-10-22T10:00:00.000000Z", "payload": {...}}
```

## Install

```bash
pip install kombu-eventbus
# or, straight from git:
pip install "kombu-eventbus @ git+https://github.com/khandroider/kombu-eventbus@v0.1.0"
```

Only dependency: `kombu` (already present wherever Celery is).

## Publish

```python
from kombu_eventbus import EventBus

bus = EventBus("amqp://user:pass@broker:5672//", source="taphr_ai")
bus.publish("resume_parsed", {"requestId": "...", "result": {...}, "error": None})
# -> routing key taphr_ai.resume_parsed, persistent, publisher confirm awaited
```

## Consume

```python
bus = EventBus(
    "amqp://user:pass@broker:5672//",
    source="taphr_ai",
    inbox_queue="taphr_ai.inbox",
    bindings=["taphr.resume_parse_requested", "crm.#"],
)

def handle(envelope):
    print(envelope.type, envelope.payload)

bus.consume(handle)  # blocks; reconnects after a broker outage
```

What the handler does decides the acknowledgement:

| handler | broker |
|---|---|
| returns | ack |
| raises `kombu_eventbus.Requeue` | message back on the queue, short pause (transient failure) |
| raises anything else | reject without requeue, traceback logged (set a dead-letter exchange on the queue to keep such messages) |

A body that is not our envelope is acked and logged. Delivery is at-least-once and
the library keeps no state: deduplicate by `envelope.id` in the application if a
repeat is not harmless.

## Celery

Inbound events become ordinary Celery tasks, so workers, retries and failure
bookkeeping stay as they are:

```python
from kombu_eventbus.celery import CeleryRouter, Route

router = CeleryRouter(celery_app, {
    "resume_parse_requested": Route(
        task="/resume_parser_v2/parse",
        queue="resume_parser_v2",
        kwargs=lambda p: {"request_body": {"request_id": p["requestId"], "file_url": p["fileUrl"]}},
    ),
})
bus.consume(router)
```

The queue is named in the route because the task is sent by name: the consumer
process does not import the task modules. An unmapped type or a payload without
the expected keys is dropped with a log line; a Celery broker outage raises
`Requeue`, so the message waits.

## Options

`EventBus(url, source, *, exchange="events", inbox_queue=None, bindings=(), heartbeat=60,
prefetch_count=10, reconnect_delay=5.0, requeue_pause=1.0)`. `consume(handler, stop=threading.Event())`
lets you end the loop from a signal handler or a test.

## Tests

```bash
pytest                                     # offline
EVENTBUS_TEST_URL=amqp://guest:guest@localhost:5672// pytest   # plus one round trip through a real broker
```

## License

MIT
