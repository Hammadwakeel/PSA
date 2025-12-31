# RiverGen AI Engine API

A high-performance FastAPI wrapper for the RiverGen AI logic, capable of routing and executing queries across SQL, Vector, and Streaming (Kafka/Kinesis) data sources.

## 🚀 Features
- **Master Router**: Automatically directs prompts based on intent and source type.
- **Dialect Awareness**: Handles Kinesis Shards and Kafka Topics dynamically.
- **Stream Analytics**: Supports windowing, moving averages, and anomaly detection.
- **Pydantic Validation**: Strict schema enforcement for data source payloads.

## 🛠️ Folder Structure
```text
app/
├── main.py              # FastAPI Entry point
├── core/
│   ├── config.py        # Environment & Model settings
│   └── agents.py        # Specialized Agent logic (SQL, Vector, Stream)
├── services/
│   └── rivergen.py      # Core workflow orchestrator
├── routers/
│   └── execution.py     # API Endpoints
└── schemas/
    └── payload.py       # Input/Output validation models