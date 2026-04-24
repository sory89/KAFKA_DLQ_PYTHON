import os

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC_RAW:       str = os.getenv("TOPIC_RAW",       "raw-events")
TOPIC_PROCESSED: str = os.getenv("TOPIC_PROCESSED", "processed-events")
TOPIC_DLQ:       str = os.getenv("TOPIC_DLQ",       "dead-letter-queue")

# ── Consumer ──────────────────────────────────────────────────────────────────
CONSUMER_GROUP: str   = os.getenv("CONSUMER_GROUP", "dlq-demo-consumer")
FAILURE_RATE:   float = float(os.getenv("FAILURE_RATE", "0.2"))

# ── Producer ──────────────────────────────────────────────────────────────────
API_URL:                  str   = os.getenv("API_URL", "https://jsonplaceholder.typicode.com/posts")
PRODUCE_INTERVAL_SECONDS: float = float(os.getenv("PRODUCE_INTERVAL_SECONDS", "5"))

# ── PostgreSQL ────────────────────────────────────────────────────────────────
DB_DSN: str = os.getenv(
    "DB_DSN",
    "host=postgres port=5432 dbname=dlq user=dlq password=dlq"
)
