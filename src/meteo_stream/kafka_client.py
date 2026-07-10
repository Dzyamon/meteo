from __future__ import annotations

import json
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from meteo.config import get_settings


def create_producer() -> KafkaProducer:
    settings = get_settings()
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        acks="all",
        retries=3,
    )


def create_consumer() -> KafkaConsumer:
    settings = get_settings()
    return KafkaConsumer(
        settings.kafka_topic_observations,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )


def publish_event(producer: KafkaProducer, topic: str, key: str, event: dict[str, Any]) -> None:
    future = producer.send(topic, key=key, value=event)
    future.get(timeout=30)
    producer.flush()
