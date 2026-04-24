"""
Fetches posts from a public API, optionally corrupts them to simulate
bad data, then publishes to the raw-events Kafka topic.
"""

import json
import logging
import random
import time

import httpx
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from .config import API_URL, KAFKA_BOOTSTRAP, PRODUCE_INTERVAL_SECONDS, TOPIC_RAW

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
log = logging.getLogger("producer")


def build_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
                linger_ms=50,
                acks="all",
            )
        except NoBrokersAvailable:
            log.info("Kafka not ready, retrying in 2s…")
            time.sleep(2)


def fetch_posts() -> list[dict]:
    with httpx.Client(timeout=10.0) as client:
        r = client.get(API_URL)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else [data]


def maybe_corrupt(record: dict) -> dict:
    """Randomly corrupt a record to exercise downstream error handling."""
    roll = random.random()
    if roll < 0.10:
        bad = dict(record)
        bad.pop("userId", None)          # missing required field
        return bad
    if roll < 0.18:
        bad = dict(record)
        bad["id"] = "not-an-int"         # wrong type
        return bad
    if roll < 0.22:
        bad = dict(record)
        bad["body"] = "X" * 5000         # oversized body
        return bad
    return record


def main():
    producer = build_producer()
    log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)

    while True:
        try:
            posts = fetch_posts()
        except Exception as e:  # noqa: BLE001
            log.warning("API fetch failed: %s", e)
            time.sleep(PRODUCE_INTERVAL_SECONDS)
            continue

        random.shuffle(posts)
        batch = posts[:10]

        for post in batch:
            payload = maybe_corrupt(post)
            key = str(payload.get("id", random.randint(1, 1_000_000)))
            producer.send(TOPIC_RAW, key=key, value=payload)

        producer.flush()
        log.info("Produced %d messages to %s", len(batch), TOPIC_RAW)
        time.sleep(PRODUCE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
