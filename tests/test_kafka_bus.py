from __future__ import annotations

from fleet_streaming.ingestion.kafka_queue import InProcessKafkaQueue


def test_kafka_publish_consume_fifo() -> None:
    bus = InProcessKafkaQueue()
    topic = "rental.cdc.v1"
    bus.publish(topic, {"rental_id": "a"})
    bus.publish(topic, {"rental_id": "b"})
    first = bus.consume(topic)
    second = bus.consume(topic)
    assert first is not None and first["rental_id"] == "a"
    assert second is not None and second["rental_id"] == "b"
    assert bus.consume(topic) is None


def test_kafka_depth() -> None:
    bus = InProcessKafkaQueue()
    topic = "rental.cdc.v1"
    bus.publish(topic, {"rental_id": "x"})
    assert bus.depth(topic) == 1
    bus.consume(topic)
    assert bus.depth(topic) == 0
