"""
Reads raw events, validates/processes records, routes failures to DLQ.
"""

import json
import logging
import random
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from pydantic import BaseModel, Field, ValidationError

from .config import (
    CONSUMER_GROUP,
    FAILURE_RATE,
    KAFKA_BOOTSTRAP,
    TOPIC_DLQ,
    TOPIC_PROCESSED,
    TOPIC_RAW,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [consumer] %(message)s")
log = logging.getLogger("consumer")


class Post(BaseModel):
    userId: int
    id: int
    title: str = Field(min_length=1)
    body: str = Field(min_length=1, max_length=2000)


class TransientError(Exception):
    """Simulates a flaky downstream call."""


def process(record: dict) -> dict:
    post = Post(**record)

    if random.random() < FAILURE_RATE:
        raise TransientError("downstream service returned 503")

    return {
        "id": post.id,
        "userId": post.userId,
        "title_word_count": len(post.title.split()),
        "body_word_count": len(post.body.split()),
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


def build_consumer() -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                TOPIC_RAW,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=CONSUMER_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
        except NoBrokersAvailable:
            log.info("Kafka not ready, retrying in 2s…")
            time.sleep(2)


def build_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k else None,
                acks="all",
            )
        except NoBrokersAvailable:
            log.info("Kafka not ready, retrying in 2s…")
            time.sleep(2)


def to_headers(d: dict) -> list[tuple[str, bytes]]:
    return [(k, str(v).encode("utf-8")) for k, v in d.items() if v is not None]


def send_dlq(producer: KafkaProducer, msg, err: Exception):
    error_type = type(err).__name__
    error_message = err.json() if isinstance(err, ValidationError) else str(err)

    dlq_payload = {
        "original_payload": msg.value,
        "error_type": error_type,
        "error_message": error_message,
        "source_topic": msg.topic,
        "source_partition": msg.partition,
        "source_offset": msg.offset,
        "message_key": msg.key,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }

    producer.send(
        TOPIC_DLQ,
        key=msg.key,
        value=dlq_payload,
        headers=to_headers({
            "x-error-type": error_type,
            "x-source-topic": msg.topic,
            "x-source-partition": msg.partition,
            "x-source-offset": msg.offset,
        }),
    )


def main():
    consumer = build_consumer()
    producer = build_producer()

    log.info(
        "Consumer started — group=%s topic=%s failure_rate=%.2f",
        CONSUMER_GROUP, TOPIC_RAW, FAILURE_RATE,
    )

    for msg in consumer:
        try:
            result = process(msg.value)
            producer.send(TOPIC_PROCESSED, key=msg.key, value=result)
            log.info("OK  key=%s offset=%d", msg.key, msg.offset)
        except Exception as e:  # noqa: BLE001
            log.warning("FAIL key=%s offset=%d err=%s", msg.key, msg.offset, e)
            send_dlq(producer, msg, e)


if __name__ == "__main__":
    main()
