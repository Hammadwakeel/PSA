# 🌊 RiverGen AI Engine

The RiverGen AI Engine is a high-performance orchestration API that translates natural language prompts into executable plans for **SQL**, **NoSQL**, **Streaming**, **Vector Databases**, and **Machine Learning** workflows.

## 🚀 Key Features

* **Intelligent Routing**: Automatically dispatches requests to specialized agents based on data source types and prompt intent.
* **Federated Execution**: Joins data across multiple sources (e.g., PostgreSQL + S3) using hybrid Trino-SQL strategies.
* **Automated QA (LLM Judge)**: A self-correcting validation loop where an LLM Judge audits generated plans for schema compliance and safety before execution.
* **Governance & RLS**: Programmatically injects Row Level Security (RLS) and column masking based on user context.
* **Production-Ready API**: Built with FastAPI, utilizing asynchronous lifespan management and thread-safe execution for heavy LLM workloads.

---

## 📂 Project Structure

```text
app/
├── main.py              # FastAPI Entry point & Middleware
├── core/
│   ├── config.py        # Lazy-loaded configuration & LLM clients
│   └── agents.py        # Specialized Agent logic (SQL, NoSQL, Vector, etc.)
├── services/
│   └── rivergen.py      # Core workflow orchestrator (Routing -> Judge Loop)
├── routers/
│   └── execution.py     # API Endpoints with thread-pool offloading
└── schemas/
    └── payload.py       # Pydantic validation models for requests/responses

```

---

## 🛠️ Architecture Overview

The engine follows a **Routing -> Generation -> Validation** lifecycle:

1. **Request Ingestion**: The API receives an `ExecutionRequest` containing user context, data source schemas, and the natural language prompt.
2. **Master Router**: Analyzes the payload to select the optimal specialized agent (e.g., `sql_agent` for relational data or `ml_agent` for training tasks).
3. **Plan Generation**: The selected agent constructs a detailed execution plan containing the specific query or workflow logic.
4. **The Judge (Self-Correction)**: The `llm_judge` validates the plan against the original schema. If rejected, it provides feedback for the agent to self-correct (up to 3 retries).
5. **Final Response**: Returns a validated, dialect-aware execution plan along with token usage and AI metadata.

---

## 🔧 Installation & Setup

### 1. Prerequisites

* Python 3.9+
* Groq API Key

### 2. Environment Configuration

Create a `.env` file in the root directory:

```env
GROQ_API_KEY=your_api_key_here
MODEL_NAME=meta-llama/llama-3.3-70b-versatile
```

### 3. Run the Application

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload

```

---

## 📡 API Endpoints

| Method | Endpoint | Description |
| --- | --- | --- |
| `POST` | `/api/v1/execute` | Primary execution endpoint for NL-to-Plan conversion. |
| `GET` | `/health` | Dynamic health check with system status. |
| `GET` | `/docs` | Interactive Swagger UI documentation. |

---

## 🛡️ Governance & Security

The system enforces enterprise security through:

* **User Context Mapping**: All queries are scoped to the `user_id` and `organization_id` provided in the payload.
* **RLS Injection**: The SQL and Multi-Source agents force literal values into filters to prevent data leakage.
* **PII Masking**: Agents are instructed to redact fields marked as `pii: true` in the schema metadata.

