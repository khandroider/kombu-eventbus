"""One round trip through a real broker. Needs EVENTBUS_TEST_URL, skipped otherwise."""
import os
import threading
import uuid

import pytest

from kombu_eventbus import EventBus

URL = os.environ.get("EVENTBUS_TEST_URL")
pytestmark = pytest.mark.skipif(not URL, reason="EVENTBUS_TEST_URL not set")


def test_publish_then_consume_round_trip():
    suffix = uuid.uuid4().hex[:8]
    publisher = EventBus(URL, source=f"pub_{suffix}", exchange=f"events_test_{suffix}")
    consumer = EventBus(URL, source=f"sub_{suffix}", exchange=f"events_test_{suffix}",
                        inbox_queue=f"sub_{suffix}.inbox", bindings=[f"pub_{suffix}.#"], heartbeat=5)
    received, stop = [], threading.Event()

    def handler(envelope):
        received.append(envelope)
        stop.set()

    thread = threading.Thread(target=consumer.consume, args=(handler,), kwargs={"stop": stop}, daemon=True)
    thread.start()
    import time
    time.sleep(1.0)  # let the consumer declare and bind its queue before publishing
    message_id = publisher.publish("ping", {"n": 1})
    assert stop.wait(10), "message did not arrive"
    thread.join(5)

    (envelope,) = received
    assert envelope.id == message_id
    assert envelope.type == "ping" and envelope.source == f"pub_{suffix}" and envelope.payload == {"n": 1}

    # cleanup the throwaway exchange and queue
    from kombu import Connection, Exchange, Queue
    with Connection(URL) as connection:
        channel = connection.channel()
        Queue(f"sub_{suffix}.inbox", channel=channel).delete()
        Exchange(f"events_test_{suffix}", channel=channel).delete()
