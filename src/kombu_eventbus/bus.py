"""Publish and consume events with a shared JSON envelope.

Wire format (the same as khandroider/laravel-eventbus):

    exchange     topic, durable (default ``events``)
    routing key  {source}.{type}                 e.g. crm.deal_won
    body         {"version": 1, "id": "<uuid>", "type": "deal_won", "source": "crm",
                  "occurred_at": "2026-10-22T10:00:00.000000Z", "payload": {...}}

Delivery is at-least-once: a consumer must tolerate a repeated message. This
library keeps no state, so deduplication by ``id`` is the application's job.
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from kombu import Connection, Consumer, Exchange, Producer, Queue, binding
from kombu.message import Message

__all__ = ["Envelope", "EventBus", "Requeue"]

ENVELOPE_VERSION = 1
_OCCURRED_AT_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

log = logging.getLogger("kombu_eventbus")


class Requeue(Exception):
    """Raise from a handler to put the message back on the queue (transient failure)."""


@dataclass(frozen=True)
class Envelope:
    """One message on the bus."""

    type: str
    payload: dict
    source: str | None = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    occurred_at: str = field(default_factory=lambda: _dt.datetime.now(_dt.timezone.utc).strftime(_OCCURRED_AT_FORMAT))
    version: int = ENVELOPE_VERSION

    @classmethod
    def from_body(cls, body: dict | str | bytes) -> "Envelope":
        """Parse a received body. Raises ValueError when it is not our envelope."""
        try:
            data = body if isinstance(body, dict) else json.loads(body)
        except (TypeError, json.JSONDecodeError) as error:
            raise ValueError(f"body is not JSON: {error}") from error
        if not isinstance(data, dict):
            raise ValueError("body is not an object")
        event_type = data.get("type")
        if not isinstance(event_type, str) or not event_type:
            raise ValueError("type is required")
        payload = data.get("payload") or {}
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        message_id = data.get("id")
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("id is required")
        return cls(
            type=event_type,
            payload=payload,
            source=data.get("source"),
            id=message_id,
            occurred_at=str(data.get("occurred_at") or ""),
            version=int(data.get("version") or ENVELOPE_VERSION),
        )

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "id": self.id,
            "type": self.type,
            "source": self.source,
            "occurred_at": self.occurred_at,
            "payload": self.payload,
        }


Handler = Callable[[Envelope], None]


class EventBus:
    """Publisher and consumer bound to one broker, one exchange and one service name.

    ``source`` is this service's name on the bus: the ``source`` field of every
    envelope it publishes and the first segment of the routing key.
    ``inbox_queue`` and ``bindings`` describe what it listens to; both may be
    left out for a publish-only service.
    """

    def __init__(
        self,
        url: str,
        source: str,
        *,
        exchange: str = "events",
        inbox_queue: str | None = None,
        bindings: Iterable[str] = (),
        heartbeat: int = 60,
        prefetch_count: int = 10,
        reconnect_delay: float = 5.0,
        requeue_pause: float = 1.0,
    ) -> None:
        self.url = url
        self.source = source
        self.exchange = Exchange(exchange, type="topic", durable=True)
        self.inbox_queue = inbox_queue
        self.bindings = tuple(bindings)
        self.heartbeat = heartbeat
        self.prefetch_count = prefetch_count
        self.reconnect_delay = reconnect_delay
        self.requeue_pause = requeue_pause

    # -- publishing ---------------------------------------------------------

    def envelope(self, event_type: str, payload: dict) -> Envelope:
        return Envelope(type=event_type, payload=payload, source=self.source)

    def publish(self, event_type: str, payload: dict) -> str:
        """Publish one event with publisher confirms; returns the message id.

        Raises the kombu/amqp error when the broker is unreachable or rejects
        the message. Opens a connection per call: fine for event-rate traffic,
        wrap in your own pooling if you publish in a tight loop.
        """
        envelope = self.envelope(event_type, payload)
        with self._connection() as connection:
            Producer(connection.channel(), exchange=self.exchange).publish(
                envelope.to_dict(),
                routing_key=f"{self.source}.{event_type}",
                serializer="json",
                delivery_mode=2,
                message_id=envelope.id,
                type=event_type,
                app_id=self.source,
                declare=[self.exchange],
                retry=True,
            )
        log.info("[EVENTBUS] Published %s id=%s", event_type, envelope.id)
        return envelope.id

    # -- consuming ----------------------------------------------------------

    def consume(self, handler: Handler, *, stop: threading.Event | None = None) -> None:
        """Block and feed every inbound envelope to ``handler``. Reconnects after a broker outage.

        Handler outcome → broker acknowledgement:
          returns            ack
          raises Requeue     message goes back to the queue, short pause
          raises otherwise   reject without requeue (dead-letter exchange if the
                             queue has one), error logged with traceback
        A body that is not our envelope is acked and logged: a retry cannot fix it.
        ``stop`` lets a caller (tests, a signal handler) end the loop.
        """
        if not self.inbox_queue:
            raise ValueError("inbox_queue is required to consume")
        queue = Queue(
            self.inbox_queue,
            durable=True,
            bindings=[binding(self.exchange, routing_key=key) for key in self.bindings],
        )
        stop = stop or threading.Event()

        def callback(body: object, message: Message) -> None:
            self._handle(body, message, handler)

        while not stop.is_set():
            try:
                with self._connection() as connection:
                    with Consumer(connection, queues=[queue], callbacks=[callback], accept=["json"], prefetch_count=self.prefetch_count):
                        log.info("[EVENTBUS] Consuming %s bindings=%s", self.inbox_queue, list(self.bindings))
                        while not stop.is_set():
                            try:
                                connection.drain_events(timeout=1)
                            except socket.timeout:
                                connection.heartbeat_check()
            except KeyboardInterrupt:
                return
            except Exception as error:  # noqa: BLE001 - any transport error: back off and reconnect
                if stop.is_set():
                    return
                log.error("[EVENTBUS] Broker connection lost, reconnecting in %ss: %s", self.reconnect_delay, error)
                stop.wait(self.reconnect_delay)

    def _handle(self, body: object, message: Message, handler: Handler) -> None:
        try:
            envelope = Envelope.from_body(body)  # type: ignore[arg-type]
        except ValueError as error:
            log.warning("[EVENTBUS] Unreadable inbound message dropped: %s", error)
            message.ack()
            return
        try:
            handler(envelope)
        except Requeue as error:
            log.error("[EVENTBUS] %s id=%s requeued: %s", envelope.type, envelope.id, error)
            message.requeue()
            time.sleep(self.requeue_pause)
            return
        except Exception:  # noqa: BLE001 - reported, the message is not retried
            log.exception("[EVENTBUS] %s id=%s rejected: handler failed", envelope.type, envelope.id)
            message.reject(requeue=False)
            return
        message.ack()

    # -- internals ----------------------------------------------------------

    def _connection(self) -> Connection:
        return Connection(self.url, heartbeat=self.heartbeat, transport_options={"confirm_publish": True})
