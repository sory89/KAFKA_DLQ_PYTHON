# 🚀 KAFKA_DLQ_PYTHON

> Inspect, manage and replay Kafka Dead Letter Queue (DLQ) messages with a FastAPI web interface.

![Architecture](https://github.com/user-attachments/assets/f5fed0b0-37fc-4b47-b637-4bcacbb32e4e)

---

## 🧠 Overview

This project demonstrates a **modern data pipeline with Kafka and DLQ pattern**.

It allows you to:
- consume and process events
- capture failures automatically
- store them in PostgreSQL
- inspect and replay messages via UI or API

👉 This mimics real-world **Data Platform / DataOps systems**

---

## 🏗️ Architecture

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/8176710c-06db-4817-8837-e15fd32e0dd5" />


---

## ⚙️ Tech Stack

- 🐍 Python 3.11
- ⚡ FastAPI
- 📨 Kafka
- 🐘 PostgreSQL
- 🎨 Jinja2 (UI)
- 🐳 Docker / Docker Compose

---

## ✨ Features

- ✅ Kafka producer & consumer
- ✅ Schema validation (Pydantic)
- ✅ Automatic DLQ routing on failure
- ✅ DLQ storage (PostgreSQL)
- ✅ Web UI to inspect messages
- ✅ Replay messages back to Kafka
- ✅ REST API for automation
- ✅ Error tracking (type, message, offset)

---

## 📦 Installation

```bash
git clone https://github.com/your-repo/KAFKA_DLQ_PYTHON.git
cd KAFKA_DLQ_PYTHON
docker compose up -d

