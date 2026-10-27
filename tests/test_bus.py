"""Offline: envelope parsing and the ack / requeue / reject rules."""
import json
import uuid

import pytest

from kombu_eventbus import Envelope, EventBus, Requeue


class FakeMessage:
    def __init__(self):
        self.acked = self.requeued = self.rejected = False

    def ack(self):
        self.acked = True

    def requeue(self):
        self.requeued = True

    def reject(self, requeue=False):
        self.rejected = True
        assert requeue is False


def bus(**kw):
    return EventBus("amqp://guest:guest@localhost//", source="svc", inbox_queue="svc.inbox", requeue_pause=0, **kw)


def body(**overrides):
    data = {"version": 1, "id": str(uuid.uuid4()), "type": "thing_happened", "source": "other",
            "occurred_at": "2026-10-22T10:00:00.000000Z", "payload": {"a": 1}}
    data.update(overrides)
    return data


def test_envelope_round_trip():
    envelope = bus().envelope("thing_happened", {"a": 1})
    parsed = Envelope.from_body(json.dumps(envelope.to_dict()))
    assert parsed == envelope
    assert parsed.source == "svc" and parsed.version == 1
    uuid.UUID(parsed.id)
    assert parsed.occurred_at.endswith("Z")


def test_from_body_accepts_dict_str_and_bytes():
    data = body()
    for raw in (data, json.dumps(data), json.dumps(data).encode()):
        assert Envelope.from_body(raw).payload == {"a": 1}


@pytest.mark.parametrize("raw", ["not json", "[]", {"version": 1}, body(type=""), body(id=None), body(payload=[1])])
def test_from_body_rejects_foreign_bodies(raw):
    with pytest.raises(ValueError):
        Envelope.from_body(raw)


def test_handler_return_acks():
    seen, message = [], FakeMessage()
    bus()._handle(body(), message, seen.append)
    assert message.acked and not message.requeued and not message.rejected
    assert seen[0].type == "thing_happened" and seen[0].payload == {"a": 1}


def test_requeue_exception_requeues():
    message = FakeMessage()

    def handler(_):
        raise Requeue("broker down")

    bus()._handle(body(), message, handler)
    assert message.requeued and not message.acked and not message.rejected


def test_other_exception_rejects_without_requeue():
    message = FakeMessage()

    def handler(_):
        raise RuntimeError("bug")

    bus()._handle(body(), message, handler)
    assert message.rejected and not message.acked and not message.requeued


def test_unreadable_body_is_acked_and_not_handled():
    message = FakeMessage()
    bus()._handle("garbage", message, lambda _: pytest.fail("must not be called"))
    assert message.acked


def test_consume_requires_inbox_queue():
    with pytest.raises(ValueError):
        EventBus("amqp://x//", source="svc").consume(lambda _: None)
