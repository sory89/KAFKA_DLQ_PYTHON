import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from .config import KAFKA_BOOTSTRAP, TOPIC_RAW
from .db import (
    counts,
    delete_dlq,
    get_dlq,
    init_db,
    list_dlq,
    mark_replayed,
    mark_resolved,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [api] %(message)s")
log = logging.getLogger("api")

BASE_DIR  = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="DLQ Review", description="Inspect and replay Kafka DLQ messages")

_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is not None:
        return _producer

    last_err = None
    for _ in range(15):
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
                acks="all",
            )
            return _producer
        except NoBrokersAvailable as e:
            last_err = e
            time.sleep(2)

    raise RuntimeError(f"Kafka unavailable after retries: {last_err}")


@app.on_event("startup")
def _startup():
    init_db()
    log.info("Database initialised")


# ── HTML routes ───────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def index(request: Request, status: str | None = Query(default=None)):
    return templates.TemplateResponse("index.html", {
        "request":       request,
        "rows":          list_dlq(status=status),
        "counts":        counts(),
        "status_filter": status or "all",
    })


@app.get("/messages/{msg_id}", response_class=HTMLResponse)
def detail(request: Request, msg_id: int):
    row = get_dlq(msg_id)
    if not row:
        raise HTTPException(404, "Message not found")

    try:
        row["payload"] = json.dumps(json.loads(row["payload"]), indent=2)
    except Exception:
        pass

    return templates.TemplateResponse("detail.html", {"request": request, "row": row})


@app.post("/messages/{msg_id}/replay")
def replay(msg_id: int, edited_payload: str | None = Form(default=None)):
    row = get_dlq(msg_id)
    if not row:
        raise HTTPException(404, "Message not found")

    payload_str = edited_payload if edited_payload is not None else row["payload"]

    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError as e:
        raise HTTPException(400, f"Invalid JSON: {e}")

    producer = get_producer()
    producer.send(TOPIC_RAW, key=row.get("message_key"), value=payload)
    producer.flush()

    mark_replayed(msg_id, datetime.now(timezone.utc).isoformat())
    log.info("Replayed DLQ id=%s → %s", msg_id, TOPIC_RAW)

    return RedirectResponse(url=f"/messages/{msg_id}", status_code=303)


@app.post("/messages/{msg_id}/resolve")
def resolve(msg_id: int):
    if not get_dlq(msg_id):
        raise HTTPException(404, "Message not found")
    mark_resolved(msg_id)
    return RedirectResponse(url="/", status_code=303)


@app.post("/messages/{msg_id}/delete")
def delete(msg_id: int):
    if not get_dlq(msg_id):
        raise HTTPException(404, "Message not found")
    delete_dlq(msg_id)
    return RedirectResponse(url="/", status_code=303)


# ── JSON API ──────────────────────────────────────────────────────────────────

@app.get("/api/dlq")
def api_list(status: str | None = None, limit: int = 200):
    return JSONResponse({"counts": counts(), "messages": list_dlq(status, limit)})


@app.get("/api/dlq/{msg_id}")
def api_get(msg_id: int):
    row = get_dlq(msg_id)
    if not row:
        raise HTTPException(404, "Message not found")
    return row


@app.post("/api/dlq/{msg_id}/replay")
def api_replay(msg_id: int):
    row = get_dlq(msg_id)
    if not row:
        raise HTTPException(404, "Message not found")

    try:
        payload = json.loads(row["payload"])
    except json.JSONDecodeError as e:
        raise HTTPException(400, f"Stored payload is not valid JSON: {e}")

    producer = get_producer()
    producer.send(TOPIC_RAW, key=row.get("message_key"), value=payload)
    producer.flush()

    mark_replayed(msg_id, datetime.now(timezone.utc).isoformat())
    return {"ok": True, "id": msg_id, "replayed_to": TOPIC_RAW}
