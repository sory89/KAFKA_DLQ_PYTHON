"""
Consumes the dead-letter topic and persists each failed message to SQLite
so the FastAPI UI can display, replay, and resolve them.
"""

import json
import logging
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from .config import CONSUMER_GROUP, KAFKA_BOOTSTRAP, TOPIC_DLQ
from .db import init_db, insert_dlq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [dlq-handler] %(message)s")
log = logging.getLogger("dlq-handler")


def build_consumer() -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                TOPIC_DLQ,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="dlq-demo-handler",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
        except NoBrokersAvailable:
            log.info("Kafka not ready, retrying in 2s…")
            time.sleep(2)


def main():
    init_db()
    consumer = build_consumer()
    log.info("DLQ handler started — waiting for messages on %s", TOPIC_DLQ)

    for msg in consumer:
        try:
            payload = msg.value
            record = {
                "received_at":      datetime.now(timezone.utc).isoformat(),
                "source_topic":     payload.get("source_topic"),
                "source_partition": payload.get("source_partition"),
                "source_offset":    payload.get("source_offset"),
                "message_key":      msg.key,
                "payload":          json.dumps(payload.get("original_payload", payload)),
                "error_type":       payload.get("error_type"),
                "error_message":    payload.get("error_message"),
                "attempts":         payload.get("attempts", 1),
            }
            row_id = insert_dlq(record)
            log.info(
                "Stored DLQ message id=%s topic=%s err=%s",
                row_id, record["source_topic"], record["error_type"],
            )
        except Exception as e:
            log.exception("Failed to store DLQ message: %s", e)


if __name__ == "__main__":
    main()
