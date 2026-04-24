import os
import threading
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from .config import DB_DSN

_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS dlq_messages (
    id               SERIAL PRIMARY KEY,
    received_at      TEXT    NOT NULL,
    source_topic     TEXT,
    source_partition INTEGER,
    source_offset    INTEGER,
    message_key      TEXT,
    payload          TEXT    NOT NULL,
    error_type       TEXT,
    error_message    TEXT,
    attempts         INTEGER NOT NULL DEFAULT 1,
    status           TEXT    NOT NULL DEFAULT 'pending',
    replayed_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_dlq_status ON dlq_messages(status);
"""


@contextmanager
def get_conn():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with _lock, get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)


def insert_dlq(record: dict) -> int:
    with _lock, get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dlq_messages
                  (received_at, source_topic, source_partition, source_offset,
                   message_key, payload, error_type, error_message, attempts, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                RETURNING id
                """,
                (
                    record["received_at"],
                    record.get("source_topic"),
                    record.get("source_partition"),
                    record.get("source_offset"),
                    record.get("message_key"),
                    record["payload"],
                    record.get("error_type"),
                    record.get("error_message"),
                    record.get("attempts", 1),
                ),
            )
            return cur.fetchone()[0]


def list_dlq(status: str | None = None, limit: int = 200) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if status:
                cur.execute(
                    "SELECT * FROM dlq_messages WHERE status = %s ORDER BY id DESC LIMIT %s",
                    (status, limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM dlq_messages ORDER BY id DESC LIMIT %s",
                    (limit,),
                )
            return [dict(r) for r in cur.fetchall()]


def get_dlq(msg_id: int) -> dict | None:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM dlq_messages WHERE id = %s", (msg_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def mark_replayed(msg_id: int, replayed_at: str):
    with _lock, get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE dlq_messages SET status='replayed', replayed_at=%s, attempts=attempts+1 WHERE id=%s",
                (replayed_at, msg_id),
            )


def mark_resolved(msg_id: int):
    with _lock, get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE dlq_messages SET status='resolved' WHERE id=%s", (msg_id,)
            )


def delete_dlq(msg_id: int):
    with _lock, get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM dlq_messages WHERE id=%s", (msg_id,))


def counts() -> dict:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT status, COUNT(*) AS n FROM dlq_messages GROUP BY status"
            )
            result = {"pending": 0, "replayed": 0, "resolved": 0, "total": 0}
            for r in cur.fetchall():
                result[r["status"]] = r["n"]
                result["total"] += r["n"]
            return result
