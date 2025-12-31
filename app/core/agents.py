import json
from datetime import datetime
from app.core.config import client, MODEL_NAME

# ==============================================================================
# 1. MASTER ROUTER AGENT
# ==============================================================================
def router_agent(full_payload):
    """
    Analyzes the COMPLETE input payload (User Prompt + Full Schema + Context)
    to route the request to the correct agent.
    """

    # 1. The "Proper Prompt"
    system_prompt = """
    You are the **Master Router** for the RiverGen AI Engine.

    Your goal is to analyze the **FULL JSON PAYLOAD** provided by the user.
    This payload contains:
    1. The User's Natural Language Prompt (`user_prompt`)
    2. The User's Context & Permissions (`user_context`)
    3. The Full Data Source Schema (`data_sources` -> `schemas` -> `tables` -> `columns`)
    4. Governance Policies (`governance_policies`)

    **YOUR TASK:**
    Based on the `user_prompt` and the `data_sources` definitions found in the JSON, route this request to the most appropriate execution agent.

    **AVAILABLE AGENTS:**
    - **sql_agent**: For standard relational SQL databases. Select this if there is **ONLY ONE** data source and its type is 'postgresql', 'oracle', 'sqlserver', 'sqlite', 'mysql', or 'mariadb'.
    - **nosql_agent**: For NoSQL/Document/Key-Value databases. Select this if there is **ONLY ONE** data source and its type is 'redis', 'mongodb', 'cassandra', 'dynamodb', or 'couchbase'.
    - **big_data_agent**: For data warehouses or lakes. Select this if there is **ONLY ONE** data source and its type is 'snowflake', 'redshift', 'bigquery', or 's3'.
    - **multi_source_agent**: For **Federated/Cross-Database** queries. Select this AUTOMATICALLY if the `data_sources` list contains **MORE THAN ONE** entry (e.g., PostgreSQL + Redis, or MySQL + Snowflake).
    - **vector_store_agent**: For semantic search or vector DBs. Select this if `data_sources[].type` is 'pinecone', 'weaviate', or usage of vector embeddings.
    - **ml_agent**: For machine learning tasks (predictions, classifications) not solved by simple SQL/Search.
    - **stream_agent**: For real-time streaming consumption. Select this if the `data_sources[].type` is 'kafka', 'RabbitMQ', or 'Apache Pulsar'

    **CRITICAL ROUTING LOGIC:**
    1. **Count the Data Sources**: Look at the `data_sources` array in the JSON.
       - If `len(data_sources) > 1`: **IMMEDIATELY SELECT `multi_source_agent`**. Do not select `sql_agent` even if the first source is SQL.
    2. **Check the Type**: If there is only 1 data source, check the `type` field to distinguish between `sql_agent`, `nosql_agent`, and `big_data_agent`.
    3. **Source Count**: If len(data_sources) > 1, select `multi_source_agent`.
    4. **Intent Over Type**: 
      - If the `user_prompt` contains words like 'consume', 'offset', 'partition', or 'kafka', you MUST select the `stream_agent`, even if the source type is 'vector' or 'sql'.
      - This prevents the system from trying to run streaming logic on static databases.
    5. **Type Fallback**: If no streaming intent is found, route by `data_sources[0].type`.

    **OUTPUT FORMAT:**
    Return ONLY a valid JSON object. No markdown. No conversational text.
    {
        "selected_agent": "string (one of the available agents)",
        "confidence": float (0.0 to 1.0),
        "reasoning": "string (Explain why, specifically referencing the data source count and types found)"
    }
    """

    try:
        # 2. Pass the ENTIRE payload as the user message
        payload_str = json.dumps(full_payload, indent=2)

        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": payload_str  # <--- PASSING ALL INPUT SCHEMA HERE
                }
            ],
            temperature=0,
            response_format={"type": "json_object"}
        )

        # 3. Parse and Return
        response_content = completion.choices[0].message.content
        return json.loads(response_content)

    except Exception as e:
        return {"error": str(e), "selected_agent": "error_handler"}
    
# ==============================================================================
# 2. STREAM AGENT (Hardened for Kafka/Kinesis Analytics)
# ==============================================================================
def stream_agent(payload, feedback=None):
    """
    Step 3/4 (Branch D): Generates an Analytical Streaming Execution Plan.
    Hardened for 10/10 performance: Windowing, Aggregations, and Anomaly Detection.
    """
    print(f"📡 [Stream Agent] Generating analytical consumer plan... (Feedback Loop: {bool(feedback)})")

    # 1. Structured Output Template for Complex Processing
    response_template = {
        "request_id": payload.get("request_id"),
        "status": "success",
        "intent_type": "stream_analytics",
        "execution_plan": {
            "strategy": "stream_processor",
            "type": "kafka_streams_config",
            "operations": [
                {
                    "step": 1,
                    "type": "consume_and_process",
                    "operation_type": "read_process",
                    "data_source_id": payload.get("data_sources", [{}])[0].get("data_source_id", 16),
                    "query_payload": {
                        "topic": "...",
                        "offset_strategy": "latest",
                        "windowing": {
                            "enabled": False,
                            "window_type": "tumbling", # tumbling, hopping, sliding
                            "size_seconds": 60,
                            "aggregation_functions": [] # e.g., ["avg", "sum", "count"]
                        },
                        "analytics": {
                            "calculate_moving_average": False,
                            "anomaly_detection": False,
                            "metrics": []
                        },
                        "filter_expression": {}, 
                        "limit": 1000
                    },
                    "governance_applied": {"note": "Stream encryption and PII masking applied"}
                }
            ]
        }
    }

    # 2. Extract Source & Schema Context
    data_sources = payload.get('data_sources', [])
    schema_summary = []
    known_fields = []
    
    for ds in data_sources:
        for schema in ds.get('schemas', []):
            for table in schema.get('tables', []):
                t_name = table.get('table_name')
                cols = []
                for c in table.get('columns', []):
                    cols.append(c['column_name'])
                    known_fields.append(c['column_name'])
                schema_summary.append(f"Topic: {t_name} | Fields: {', '.join(cols)}")

    # 3. Hardened 10/10 Stream Analytics Prompt
    system_prompt = f"""
    You are the **Stream Agent** for RiverGen AI. 
    You generate high-fidelity Kafka Streams or KSQL configurations. 
    
    **INPUT CONTEXT:**
    - User Prompt: "{payload.get('user_prompt')}"
    - Available Streams: {chr(10).join(schema_summary)}
    - Current Date: {datetime.now().strftime("%Y-%m-%d")}

    **STRICT 10/10 EXECUTION RULES:**

    1. **Temporal Windowing (MANDATORY)**: 
       - If "windowing" or "time windows" is mentioned, you MUST set `windowing.enabled: true`. 
       - Determine the `window_type` (e.g., 'tumbling') and `size_seconds` (default 60) based on the prompt. 

    2. **Analytical Logic**: 
       - If "moving average" is requested, set `analytics.calculate_moving_average: true`.
       - If "anomalies" are mentioned, set `analytics.anomaly_detection: true`.

    3. **Payload Filtering**: 
       - Distill prompt-based filters (e.g., "only event_type login") into the `filter_expression` block. 
       - ONLY use fields from this list: {', '.join(known_fields)}.

    4. **Consumer Mapping**: 
       - Map the schema "table_name" to the 'topic' field. 
       - Set `offset_strategy` to 'latest' unless 'history' is requested.

    **OUTPUT FORMAT:**
    Return ONLY a valid JSON object matching the provided template exactly. Do not add extra fields or conversational text.
    {json.dumps(response_template, indent=2)}
    """

    if feedback:
        system_prompt += f"\n\n🚨 **FIX PREVIOUS ERROR**: {feedback}"

    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": f"ID: {payload.get('request_id')}"}],
            temperature=0,
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)
    except Exception as e:
        return {"error": f"Stream Agent Failed: {str(e)}"}
    

# ==============================================================================
# 3. SQL AGENT (Relational DB Specialist)
# ==============================================================================
def sql_agent(payload, feedback=None):
    """
    Step 3/4: Generates a RiverGen Execution Plan for SQL Databases.
    Handles PostgreSQL, MySQL, Oracle, etc.
    Supports Self-Correction Loop via 'feedback'.
    """
    print(f"🤖 [SQL Agent] Generating query... (Feedback Loop: {bool(feedback)})")

    # 1. Define Strict Output Template
    response_template = {
        "request_id": payload.get("request_id"),
        "status": "success",
        "intent_type": "query",
        "execution_plan": {
            "strategy": "pushdown",
            "type": "sql_query",
            "operations": [
                {
                    "step": 1,
                    "type": "source_query",
                    "operation_type": "read", # or 'write'
                    "data_source_id": payload.get("data_sources", [{}])[0].get("data_source_id", 1),
                    "query": "SELECT ...",
                    "query_payload": {
                        "language": "sql",
                        "dialect": "postgresql",
                        "statement": "SELECT ..."
                    },
                    "governance_applied": {
                        "rls_rules": ["rule_name_if_applied"],
                        "masking_rules": ["column_name_if_masked"]
                    }
                }
            ]
        }
    }

    # 2. Extract Context & Schema
    data_sources = payload.get('data_sources', [])
    user_context = payload.get('user_context', {})

    # Specific context variables the LLM needs to resolve placeholders
    context_vars = {
        "user_id": user_context.get("user_id"),
        "workspace_id": user_context.get("workspace_id"),
        "org_id": user_context.get("organization_id"),
        "attributes": user_context.get("attributes", {})
    }

    schema_summary = []
    governance_context = []

    for ds in data_sources:
        ds_name = ds.get('name')

        # Schema Summary
        for schema in ds.get('schemas', []):
            for table in schema.get('tables', []):
                t_name = table.get('table_name')
                cols = [c['column_name'] for c in table.get('columns', [])]
                if cols:
                    schema_summary.append(f"Table: {t_name} | Columns: {', '.join(cols)}")
                else:
                    schema_summary.append(f"Table: {t_name} | Columns: [UNKNOWN - INFER BASED ON CONTEXT]")

        # Governance Summary
        policies = ds.get('governance_policies', {})
        if policies:
            governance_context.append(f"Source '{ds_name}': {json.dumps(policies)}")

    # 3. Build the "Proper Prompt" - ENHANCED LOGIC & ACCURACY
    system_prompt = f"""
    You are the **SQL Agent** for RiverGen AI.
    Your goal is to turn natural language into secure, executable, and **logically accurate** SQL.

    **YOUR TASK:**
    Generate a JSON Execution Plan containing a syntactically correct SQL query that answers the user's request.

    **INPUT CONTEXT:**
    - User Prompt: "{payload.get('user_prompt')}"
    - Data Source Type: "{data_sources[0].get('type') if data_sources else 'sql'}"
    - **Context Variables (FOR INJECTION):** {json.dumps(context_vars)}
    - Available Schema:
      {chr(10).join(schema_summary)}

    **GOVERNANCE POLICIES (MANDATORY):**
    {chr(10).join(governance_context) if governance_context else "No active policies."}

    **CRITICAL RULES & LOGIC:**

    1. **Dialect Handling**:
       - `postgresql`: Standard SQL, double quotes for identifiers.
       - `mysql`: Backticks (`) for identifiers.
       - `oracle`: Standard SQL.

    2. **Logical Accuracy (AVOID FAN-OUT TRAPS)**:
       - **Rule**: NEVER calculate sums (e.g. `SUM(order_amount)`) *after* joining a child table (e.g. `order_items`), as this duplicates rows and inflates the total.
       - **Fix**: Perform aggregations in a CTE or Subquery *before* joining, or use `DISTINCT` inside the aggregation if appropriate (e.g. `COUNT(DISTINCT order_id)`).

    3. **Governance Enforcement (STRICT)**:
       - **Security Inheritance**: If an RLS policy exists on a specific table (e.g. `region` column in `customers`), you **MUST** join that table to enforce the rule, even if the user query targets a child table (e.g. `orders`). You cannot bypass security by querying the child table directly.
       - **Value Injection**: You must **REPLACE** placeholders like `{{user_id}}` or `:user_id` with the **ACTUAL LITERAL VALUES** from the "Context Variables" above.
         * *Bad:* `WHERE user_id = :user_id`
         * *Good:* `WHERE user_id = 1`

    4. **Time Period Comparisons (CRITICAL)**:
       - **Rule**: When comparing "Current" vs "Previous" periods, **NEVER JOIN ON RAW DATES** (e.g., `2023-01-01` != `2022-10-01`). They will never match.
       - **Fix**: Extract an abstract index like **"Week Number of Quarter"** or "Day Number" and join on that.
       - *Example*: `ON cur.week_num = prev.week_num`

    5. **Write Operations & Safety (CRITICAL)**:
       - If `INSERT`/`UPDATE`/`DELETE` is implied, set `operation_type` to `write`.
       - **Transaction Safety**: Always wrap complex writes (multiple steps) in `BEGIN; ... COMMIT;`.
       - **Vague Deletes**: If the user says "Delete invalid data" without criteria, **INFER** standard data quality checks (e.g., `email IS NULL` or `NOT LIKE '%@%'`) based on the schema columns.
       - **Blast Radius**: ALWAYS apply the Governance (RLS) policies to `DELETE` statements to ensure users can only delete their own data.

    6. **Aggregation Logic**:
       - Ensure `GROUP BY` matches non-aggregated columns.
       - Use `ARRAY_AGG` or `JSON_AGG` to return related lists (e.g. "Order History") without exploding row counts.

    **OUTPUT FORMAT:**
    Return ONLY a valid JSON object matching this structure EXACTLY:
    {json.dumps(response_template, indent=2)}
    """

    # 4. Inject Feedback (Self-Correction Logic)
    if feedback:
        system_prompt += f"""

        🚨 **CRITICAL: FIX PREVIOUS ERROR** 🚨
        Your previous attempt was rejected.
        **FEEDBACK:** "{feedback}"

        **CORRECTION STRATEGY:**
        - If "Security/RLS" error: Ensure you joined the `customers` table to filter by `region`.
        - If "Date/Join" error: Ensure you joined on "Week Number", not "Date".
        - If "Fan-Out" error: Move SUMs to a CTE.
        - If "Vague Delete" error: Infer specific column checks (e.g. email format) and apply RLS.
        """

    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Request ID: {payload.get('request_id')}"}
            ],
            temperature=0, # Keep temp low for code generation
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)

    except Exception as e:
        return {"error": f"SQL Agent Failed: {str(e)}"}

# ==============================================================================
# 4. VECTOR STORE AGENT (Similarity & Rejection Logic)
# ==============================================================================
def vector_store_agent(payload, feedback=None):
    """
    Step 3/4 (Branch C): Generates a Vector Search Execution Plan.
    Hardened for 10/10 performance with Metadata Filter Mapping.
    """
    print(f"🎯 [Vector Agent] Generating similarity search plan... (Feedback Loop: {bool(feedback)})")

    # 1. Define Strict Output Template
    response_template = {
        "request_id": payload.get("request_id"),
        "status": "success",
        "intent_type": "vector_search",
        "execution_plan": {
            "strategy": "vector_similarity_search",
            "type": "vector_query",
            "operations": [
                {
                    "step": 1,
                    "type": "vector_search",
                    "operation_type": "read",
                    "data_source_id": payload.get("data_sources", [{}])[0].get("data_source_id", 15),
                    "query_payload": {
                        "index_name": "...",
                        "top_k": 10,
                        "vector_column": "...",
                        "include_metadata": True,
                        "include_values": False,
                        "filter": {}, # Hardened: Maps "recent" or "categories"
                        "query_vector": [], 
                        "search_params": {
                            "metric": "cosine",
                            "queries": []
                        }
                    },
                    "governance_applied": {"note": "No RLS applied to vector index"}
                }
            ]
        }
    }

    # 2. Extract Context & Schema Summary
    data_sources = payload.get('data_sources', [])
    schema_summary = []
    known_metadata_cols = []
    
    for ds in data_sources:
        for schema in ds.get('schemas', []):
            for table in schema.get('tables', []):
                t_name = table.get('table_name')
                cols = []
                for c in table.get('columns', []):
                    col_info = f"{c['column_name']} ({c['column_type']})"
                    cols.append(col_info)
                    if "vector" not in c['column_type'].lower():
                        known_metadata_cols.append(c['column_name'])
                schema_summary.append(f"Index: {t_name} | Columns: {', '.join(cols)}")

    unsupported_reason_example = json.dumps({
    "__UNSUPPORTED__": "Streaming / message-consumption semantics are not supported by vector databases"
})


    system_prompt = f"""
You are the **Vector Store Agent** for RiverGen AI.

Your responsibility is to generate an execution plan ONLY for operations that are
**natively supported by vector databases** (e.g., Pinecone, Weaviate).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SUPPORTED CAPABILITIES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
You MAY generate plans for:
- Vector similarity search
- Semantic retrieval using embeddings
- Metadata-based filtering on indexed attributes
- Top-K ranking by distance metric

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
UNSUPPORTED CAPABILITIES (CRITICAL)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Vector databases DO NOT support:
- Message consumption or streaming
- Message headers
- Topics or partitions
- Offset tracking
- Group-by key semantics
- Event stream processing

If a user request depends on ANY unsupported capability,
you MUST NOT attempt to simulate or approximate it.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INPUT CONTEXT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
User Prompt:
"{payload.get('user_prompt')}"

Available Schema:
{chr(10).join(schema_summary)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MANDATORY INTENT CLASSIFICATION (FIRST STEP)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. **Classify the user intent BEFORE planning**
   - If the intent is **vector similarity search**, proceed normally.
   - If the intent describes **streaming, message queues, events, partitions, offsets, or headers**:
     - STOP immediately.
     - Return a structured rejection.

2. **Structured Rejection Rule (MANDATORY)**
   - When rejecting, output a valid JSON plan using this rule:
     - strategy = "unsupported_operation"
     - operations MUST contain exactly one step
     - The step must include a `query_payload` with:
       "error": {unsupported_reason_example}

3. **NO RETRIES FOR UNSUPPORTED INTENT**
   - Do NOT attempt alternate plans.
   - Do NOT retry.
   - Do NOT hallucinate mappings.
   - A clean rejection is a SUCCESSFUL outcome.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VECTOR SEARCH RULES (ONLY IF SUPPORTED)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

If and ONLY IF the intent is vector similarity search:

- Extract only semantic intent for embeddings
- Include query_vector placeholder []
- Enforce cosine distance
- Enforce top_k when ranking is implied
- Never invent schema fields
- Apply metadata filters only when supported

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STRICT OUTPUT CONTRACT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- Output ONLY a single valid JSON object
- Match the provided template structure
- No explanations, markdown, or commentary

OUTPUT TEMPLATE:
{json.dumps(response_template, indent=2)}
"""

    if feedback:
        system_prompt += f"\n\n🚨 **FIX ERROR**: {feedback}"

    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": f"ID: {payload.get('request_id')}"}],
            temperature=0,
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)
    except Exception as e:
        return {"error": f"Vector Agent Failed: {str(e)}"}
    
# ==============================================================================
# 5. MULTI-SOURCE AGENT (Federated Trino/ANSI SQL)
# ==============================================================================
def multi_source_agent(payload, feedback=None):
    """
    Step 3/4 (Branch B): Generates a Federated Execution Plan.
    Hardened for 10/10 performance, security, and schema-compliance.
    """
    print(f"🌐 [Multi-Source Agent] Generating federated plan... (Feedback Loop: {bool(feedback)})")

    # 1. Strict Output Template
    response_template = {
        "request_id": payload.get("request_id"),
        "status": "success",
        "intent_type": "federated_query",
        "execution_plan": {
            "strategy": "federated_pushdown",
            "type": "trino_sql",
            "operations": [
                {
                    "step": 1,
                    "type": "virtualization_query",
                    "operation_type": "read",
                    "involved_sources": [],
                    "query": "SELECT ...",
                    "query_payload": {
                        "language": "sql",
                        "dialect": "trino",
                        "statement": "SELECT ..."
                    },
                    "governance_applied": {
                        "rls_rules": [],
                        "masking_rules": [],
                        "note": ""
                    }
                }
            ]
        }
    }

    # 2. Extract Context & Combined Schema
    data_sources = payload.get('data_sources', [])
    user_context = payload.get('user_context', {})
    context_vars = {
        "user_id": user_context.get("user_id"),
        "org_id": user_context.get("organization_id"),
        "attributes": user_context.get("attributes", {})
    }

    schema_summary = []
    governance_context = []
    source_names = []
    known_columns = set()

    for ds in data_sources:
        ds_name = ds.get('name')
        source_names.append(ds_name)
        
        # Unified Schema Extraction (SQL + S3)
        curr_schemas = ds.get('schemas') or ds.get('file_metadata', {}).get('schemas', [])
        for schema in curr_schemas:
            for table in schema.get('tables', []):
                t_name = table.get('table_name')
                cols = [c['column_name'] for c in table.get('columns', [])]
                for c in cols: known_columns.add(f"{t_name}.{c}".lower())
                schema_summary.append(f"SOURCE [{ds.get('type')}] '{ds_name}' -> Table: {t_name} | Columns: {', '.join(cols)}")

        policies = ds.get('governance_policies', {})
        if policies:
            governance_context.append(f"Policies for '{ds_name}': {json.dumps(policies)}")

    system_prompt = f"""
You are the **Multi-Source Federation Agent** responsible for generating
**secure, governed Trino SQL** for enterprise data virtualization.

Your job is to:
1. Interpret the user's analytical intent.
2. Generate SQL ONLY for queryable sources.
3. Enforce governance where feasible.
4. Transparently explain all limitations.
5. Produce an auditable execution plan.

────────────────────────────────────────
INPUT CONTEXT
────────────────────────────────────────
- User Prompt:
  "{payload.get('user_prompt')}"

- Context Literals (trusted runtime values):
  {json.dumps(context_vars)}

- Available Schema (AUTHORITATIVE — hard boundary):
  {chr(10).join(schema_summary)}

- Governance Policies:
  {chr(10).join(governance_context) if governance_context else "None."}

────────────────────────────────────────
ABSOLUTE RULES (NON-NEGOTIABLE)
────────────────────────────────────────

1. **Schema Authority (HARD RULE)**
   - You MAY ONLY reference tables and columns listed in **Available Schema**.
   - Any reference outside this list is a hallucination and MUST be avoided.

2. **Queryable vs Non-Queryable Sources (CRITICAL)**
   - A data source is QUERYABLE **only if it appears in Available Schema**.
   - If a source appears in the input payload but NOT in Available Schema:
     - Treat it as **NON-QUERYABLE**
     - DO NOT generate SQL for it
     - DO NOT reference it in any identifier
     - Explicitly list it under `dropped_sources` with explanation

3. **Federation Requirement**
   - You MUST federate ALL QUERYABLE sources relevant to the user prompt.
   - Federation is NOT required for non-queryable sources.

4. **Governance Enforcement**
   - Apply governance rules ONLY when they are enforceable using Available Schema.
   - If a governance rule references missing tables or columns:
     - Attempt literal substitution using Context Literals if possible
     - Otherwise OMIT the rule and DOCUMENT the omission
   - Silent omission is forbidden.

5. **Row-Level Security Injection**
   - Apply RLS filters inside the relevant source subquery.
   - NEVER introduce joins solely to enforce governance.

6. **Join Strategy**
   - Join ONLY on real business keys present in schema.
   - CAST types explicitly when needed.
   - If no valid join exists, do NOT fabricate one.

7. **Pushdown Optimization**
   - Apply filters, governance, and LIMITS inside each source subquery
     before joining.

8. **Unified Metrics**
   - Use `COALESCE(value, 0)` when combining metrics across sources.

9. **SQL Dialect**
   - Trino / ANSI SQL only.
   - No vendor-specific extensions.

10. **Failure Transparency**
    - If the full user intent cannot be satisfied:
      - Generate the best valid plan possible
      - Mark the outcome explicitly
      - Explain all limitations

────────────────────────────────────────
REQUIRED OUTPUT SEMANTICS
────────────────────────────────────────

Return ONLY valid JSON matching this structure exactly:
{json.dumps(response_template, indent=2)}

Additionally, the JSON MUST include:

- `execution_outcome`:
  - One of: SAFE_FULL | SAFE_PARTIAL | REJECTED

- `dropped_sources`:
  - List of non-queryable sources (empty if none)

- `governance_enforcement`:
  - Per policy: enforced | partially_enforced | omitted + explanation

- `limitations`:
  - Human-readable unmet intent explanations

- `quality_rating`:
  - Integer 1–5 based on completeness, safety, and transparency

────────────────────────────────────────
QUALITY BAR
────────────────────────────────────────

★★★★★ (5)
- All queryable sources federated
- Governance enforced or transparently omitted
- No hallucinations
- Full transparency

★★★★☆ (4)
- SAFE_PARTIAL with clear explanations

≤ ★★★☆☆
- Missing explanations, weak reasoning, or unsafe behavior

DO NOT include markdown.
DO NOT include explanations outside JSON.
DO NOT hallucinate.
"""

    if feedback:
      system_prompt += f"""
🚨 PREVIOUS ERROR DETECTED 🚨

The last attempt was REJECTED because it referenced an INVALID table or column:
"{feedback}"

MANDATORY CORRECTION RULES:
- You MUST NOT reference "{feedback}" in any form.
- You MUST NOT reference any table or column derived from "{feedback}".
- Treat this object as NON-QUERYABLE.
- If it belongs to a data source, that source MUST be listed under dropped_sources.
- Generate SQL ONLY using objects that appear in the Available Schema.

Failure to comply will result in rejection.
"""

    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": f"ID: {payload.get('request_id')}"}],
            temperature=0,
            response_format={"type": "json_object"}
        )
        res = json.loads(completion.choices[0].message.content)
        res["execution_plan"]["operations"][0]["involved_sources"] = source_names
        return res
    except Exception as e:
        return {"error": f"Agent Failed: {str(e)}"}

# ==============================================================================
# 6. LLM JUDGE (The Quality Gate)
# ==============================================================================
def llm_judge(original_payload, generated_plan):
    """
    Validates the generated execution plan against the user prompt and schema.
    Returns a JSON object with 'approved' (bool) and 'feedback' (str).

    UPDATED: Now includes 'Schema-Awareness' to prevent rejecting safe failures
    (e.g., ignoring 'active' filter if the column doesn't exist).
    """

    # 1. Extract Schema Context (Tables AND Columns)
    # We flatten the schema to a simple list of "table.column" or just "column" names
    # to help the Judge know what is actually possible.
    data_sources = original_payload.get("data_sources", [])
    valid_schema_context = []

    for ds in data_sources:
        ds_name = ds.get("name")
        for schema in ds.get("schemas", []):
            for table in schema.get("tables", []):
                table_name = table.get("table_name")
                columns = [col.get("column_name").lower() for col in table.get("columns", [])]
                valid_schema_context.append({
                    "source": ds_name,
                    "table": table_name,
                    "valid_columns": columns
                })

    # 2. The "Proper Prompt" for QA
    system_prompt = f"""
You are the **Quality Assurance Judge** for the RiverGen AI Engine.

Your role is to evaluate whether the proposed execution_plan is:
- Safe
- Schema-valid
- Logically aligned with the user prompt
- Transparently handles infeasible requirements

────────────────────────────────────────
INPUT CONTEXT
────────────────────────────────────────
1. **User Prompt:**
   "{original_payload.get("user_prompt")}"

2. **Valid Schema Definition (authoritative):**
   {json.dumps(valid_schema_context)}

3. **Proposed Execution Plan:**
   {json.dumps(generated_plan, indent=2)}

────────────────────────────────────────
VALIDATION RULES (STRICT BUT FAIR)
────────────────────────────────────────

1. **Hallucination Check (ABSOLUTE)**
   - If the execution plan references ANY table or column
     NOT explicitly present in the Valid Schema Definition:
     → You MUST reject the plan.

2. **Schema Authority Precedence**
   - The Valid Schema Definition is the ONLY source of truth.
   - Payload presence ≠ queryable schema.
   - Plans may drop non-queryable sources if explicitly documented.

3. **Federation Feasibility**
   - SAFE_PARTIAL plans are valid when limitations are explained.
   - Do NOT reject a plan solely for omitting infeasible sources.

4. **Governance & Filter Feasibility**
   - Missing schema elements justify omission of filters.
   - Prefer approval when omission is explicitly explained.

5. **Logic & Syntax**
   - SQL must be valid for the target dialect.
   - Logic must reasonably satisfy user intent within schema limits.

────────────────────────────────────────
OUTPUT FORMAT (STRICT)
────────────────────────────────────────
Return a strictly valid JSON object:

{{
  "approved": boolean,
  "feedback": "string",
  "score": float
}}

Rules:
- If approved = true → feedback MUST be "Approved"
- If approved = false → feedback MUST name the hallucinated table or column
- Do NOT include commentary outside JSON
"""

    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME, # Strong reasoning model required for QA
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": "Please validate this plan."}
            ],
            temperature=0,
            response_format={"type": "json_object"}
        )

        return json.loads(completion.choices[0].message.content)

    except Exception as e:
        return {"approved": False, "feedback": f"Judge Error: {str(e)}"}
    
################################################################################
# 7. NOSQL AGENT (NoSQL/Document DB Specialist)
################################################################################
def nosql_agent(payload, feedback=None):
    """
    Step 3/4: Generates a RiverGen Execution Plan for NoSQL Databases.
    Supported families:
    - Document (MongoDB)
    - Search / Key-Value (Redis / Redis Stack)
    - Wide Column (Cassandra / ScyllaDB)
    - Key-Value (DynamoDB)
    """

    print(f"📦 [NoSQL Agent] Generating query... (Feedback Loop: {bool(feedback)})")

    # ------------------------------------------------------------------
    # 1. Detect DB Type & Execution Context
    # ------------------------------------------------------------------
    data_sources = payload.get("data_sources", [{}])
    primary_ds = data_sources[0] if data_sources else {}
    db_type = primary_ds.get("type", "generic_nosql").lower()

    exec_ctx = payload.get("execution_context", {})
    max_rows = exec_ctx.get("max_rows", 1000)
    timeout_seconds = exec_ctx.get("timeout_seconds", 30)

    # ------------------------------------------------------------------
    # 2. MongoDB Example (SAFE – not inside f-string)
    # ------------------------------------------------------------------
    mongodb_pipeline_example = """
[
  {
    "$match": {
      "active": true,
      "profile.address.city": "Lahore",
      "items": {
        "$elemMatch": {
          "sku": "X123",
          "qty": { "$gt": 0 }
        }
      }
    }
  },
  {
    "$project": {
      "email": 0,
      "password": 0,
      "profile": 1,
      "items": 1,
      "created_at": 1
    }
  },
  { "$sort": { "created_at": -1 } },
  { "$limit": <MAX_ROWS> }
]
"""

    # ------------------------------------------------------------------
    # 3. Strict Response Template
    # ------------------------------------------------------------------
    response_template = {
        "request_id": payload.get("request_id"),
        "status": "success",
        "intent_type": "complex_analysis",
        "validation": {
            "schema_matches": True,
            "missing_fields": [],
            "notes": []
        },
        "warnings": [],
        "execution_plan": {
            "strategy": "multi_step_execution",
            "type": "nosql_command_batch",
            "operations": [
                {
                    "step": 1,
                    "type": "source_query",
                    "operation_type": "read",
                    "data_source_id": primary_ds.get("data_source_id"),
                    "query": "...",
                    "query_payload": {
                        "driver": None,
                        "operation": None,
                        "parameters": {}
                    },
                    "execution_parameters": {
                        "limit": max_rows,
                        "timeout_seconds": timeout_seconds
                    },
                    "governance_applied": {
                        "rls_rules": [],
                        "masking_rules": []
                    }
                }
            ],
            "metrics": {
                "estimated_rows": None,
                "estimated_cost": None
            }
        }
    }

    # ------------------------------------------------------------------
    # 4. Extract Schema & Governance Context
    # ------------------------------------------------------------------
    schema_summary = []
    known_fields = set()
    sensitive_fields = {"email", "password", "ssn", "credit_card", "dob"}

    for schema in primary_ds.get("schemas", []):
        for table in schema.get("tables", []):
            fields = []
            for col in table.get("columns", []):
                fields.append(f"{col['column_name']} ({col['column_type']})")
                known_fields.add(col["column_name"].lower())
            schema_summary.append(
                f"Collection: {table.get('table_name')} | Fields: {', '.join(fields)}"
            )

    governance_policies = primary_ds.get("governance_policies", {})

    # ------------------------------------------------------------------
    # 5. SYSTEM PROMPT (FINAL – SAFE & STRICT)
    # ------------------------------------------------------------------
    system_prompt = f"""
You are the NoSQL Agent for RiverGen AI.

You generate SAFE, VALIDATED, and EXECUTABLE NoSQL execution plans.
Your output is consumed directly by database drivers — correctness is mandatory.

========================
PRIMARY OBJECTIVE
========================
Translate the user prompt into one or more NoSQL operations that:
- strictly respect the provided schema,
- respect governance policies,
- avoid hallucinated fields,
- remain syntactically valid,
- and gracefully degrade when information is missing.

Return ONLY valid JSON matching the RESPONSE TEMPLATE.
DO NOT include explanations or commentary outside JSON.

========================
CONTEXT
========================
Database Type: {db_type.upper()}
User Prompt: "{payload.get('user_prompt')}"
Execution Context:
- max_rows = {max_rows}
- timeout_seconds = {timeout_seconds}

Schema:
{chr(10).join(schema_summary) if schema_summary else "No schema provided"}

Governance Policies:
{json.dumps(governance_policies) if governance_policies else "None"}

========================
ABSOLUTE RULES (NON-NEGOTIABLE)
========================

1. 🚫 STRICT SCHEMA ENFORCEMENT (HARD RULE)
- You may ONLY reference columns explicitly listed in the provided schema.
- You MUST NOT reference, probe, test, or check the existence of any field that is not in the schema.
- This includes:
  - Filters
  - Probes ($exists)
  - Projections
  - Sorting
  - Any part of the query

2. 🧠 NO HALLUCINATION (ZERO TOLERANCE)
- If the user requests a field or filter that is not in the schema (e.g., "active", "status", "location"):
  - DO NOT use it
  - DO NOT probe for it
  - DO NOT reference it in the query in any way
  - Add the field name to `validation.missing_fields`
  - Explain the omission clearly in `validation.notes`

3. ✅ SAFE DEGRADATION (EXPECTED BEHAVIOR)
- When a requested filter cannot be applied due to schema limitations:
  - Retrieve the best possible result using ONLY valid schema columns
  - This is considered correct behavior and must NOT be treated as an error

4. 📦 MONGODB PIPELINE RULES
- Use aggregation pipelines only
- `$match` must reference only valid schema fields
- `$project` must NOT mix inclusion (1) and exclusion (0)
- `$sort` must use only schema fields
- `$limit` must equal execution_context.max_rows

5. 🛡️ GOVERNANCE
- Sensitive fields (e.g., email) must be excluded or masked
- All governance actions must be recorded in `governance_applied.masking_rules`

6. 📊 TRANSPARENCY
- `validation.missing_fields` must list every user-requested field that could not be applied
- `validation.notes` must clearly explain why the plan partially fulfills the request

7. 🚫 FORBIDDEN ACTIONS
- Do NOT invent structure
- Do NOT probe unknown fields
- Do NOT attempt "best guess" logic
- Do NOT try to satisfy intent at the cost of schema correctness

========================
RESPONSE TEMPLATE
========================
{json.dumps(response_template, indent=2)}
"""



    # ------------------------------------------------------------------
    # 6. Feedback Loop Injection
    # ------------------------------------------------------------------
    if feedback:
        system_prompt += f"""

CRITICAL FIX REQUIRED:
Previous attempt failed.

FEEDBACK:
"{feedback}"

FIX RULES:
- Remove invalid fields immediately.
- Document changes in validation.notes.
"""

    # ------------------------------------------------------------------
    # 7. LLM Call
    # ------------------------------------------------------------------
    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Request ID: {payload.get('request_id')}"}
            ],
            temperature=0,
            response_format={"type": "json_object"}
        )

        return json.loads(completion.choices[0].message.content)

    except Exception as e:
        return {
            "request_id": payload.get("request_id"),
            "status": "failed",
            "error": f"NoSQL Agent Failed: {str(e)}",
            "validation": {
                "schema_matches": False,
                "missing_fields": [],
                "notes": ["Agent execution error"]
            },
            "warnings": []
        }


# ==============================================================================
# 8. BIG DATA AGENT (Hadoop/Spark Specialist)
# ==============================================================================
def big_data_agent(payload, feedback=None):
    """
    Step 3/4: Generates a RiverGen Execution Plan for Big Data workloads.
    Handles Cloud Warehouses (Snowflake, BigQuery) and Data Lakes (S3, Parquet).
    Supports Self-Correction Loop via 'feedback'.
    """
    print(f"🐘 [Big Data Agent] Generating plan... (Feedback Loop: {bool(feedback)})")

    # 1. Define Strict Output Template
    # We provide a generic structure that fits the 'rgen' spec, allowing the agent to fill
    # specific fields like 'type' (sql_query vs file_query) and 'dialect'.
    response_template = {
        "request_id": payload.get("request_id"),
        "status": "success",
        "intent_type": "query", # or 'transform'
        "execution_plan": {
            "strategy": "pushdown", # or 'internal_compute' for S3
            "type": "sql_query",    # or 'file_query'
            "operations": [
                {
                    "step": 1,
                    "type": "source_query", # or 'file_read'
                    "operation_type": "read",
                    "data_source_id": payload.get("data_sources", [{}])[0].get("data_source_id"),
                    "query": "SELECT ...",
                    "query_payload": {
                        "language": "sql",
                        "dialect": "snowflake", # or 'duckdb', 'bigquery'
                        "statement": "SELECT ..."
                    },
                    "governance_applied": {
                        "rls_rules": [],
                        "masking_rules": []
                    }
                }
            ]
        }
    }

    # 2. Extract Governance Context
    data_sources = payload.get('data_sources', [])
    governance_context = []
    source_type_hint = "unknown"

    for ds in data_sources:
        # Capture the specific type (e.g., 'snowflake', 's3') to guide the prompt
        source_type_hint = ds.get('type', 'unknown')

        policies = ds.get('governance_policies', {})
        if policies:
            governance_context.append(f"Source '{ds.get('name')}': {json.dumps(policies)}")

    # 3. Build the Detailed System Prompt
    system_prompt = f"""
    You are the **Big Data Agent** for RiverGen AI.

    **YOUR TASK:**
    Generate an optimized Execution Plan for a Big Data workload (Cloud Warehouse or Data Lake).

    **INPUT CONTEXT:**
    - User Prompt: "{payload.get('user_prompt')}"
    - Data Source Schema: {json.dumps(data_sources)}
    - Primary Source Type: "{source_type_hint}"

    **GOVERNANCE POLICIES (MUST ENFORCE):**
    {chr(10).join(governance_context) if governance_context else "No specific policies."}

    **DIALECT & OPTIMIZATION RULES:**
    1. **Snowflake**: Use `Snowflake` dialect. Support `QUALIFY`, `FLATTEN`, and strictly use defined database/schema names (e.g. `DB.SCHEMA.TABLE`).
    2. **BigQuery**: Use `BigQuery` standard SQL. Handle nested fields (`record.field`) if present. Use backticks for project.dataset.table.
    3. **Data Lakes (S3/ADLS/File)**:
       - Assume compute engine is **DuckDB** or **Trino**.
       - **Partition Pruning**: If the schema mentions `partition_columns`, YOU MUST filter by them in the `WHERE` clause if the prompt allows (e.g. "last 30 days" -> `date >= ...`).
       - Use file functions like `read_parquet('s3://...')` if applicable, or standard SQL if the view is abstracted.

    **OUTPUT FORMAT:**
    Return ONLY valid JSON matching the exact template below. Adjust `dialect` field based on the source type (e.g. 'snowflake', 'bigquery', 'duckdb').

    **OUTPUT TEMPLATE:**
    {json.dumps(response_template, indent=2)}
    """

    # 4. Inject Feedback (Self-Correction Logic)
    if feedback:
        system_prompt += f"""

        🚨 **CRITICAL: FIX PREVIOUS ERROR** 🚨
        Your previous plan was rejected by the QA Judge.
        **FEEDBACK:** "{feedback}"

        **INSTRUCTIONS FOR FIX:**
        - If you used the wrong dialect (e.g. BigQuery syntax on Snowflake), fix it.
        - If you missed a partition filter on a large table, ADD IT.
        - If you hallucinated a path or table, check the schema string again.
        """

    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME, # Uses global MODEL_NAME
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload)}
            ],
            temperature=0,
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)

    except Exception as e:
        return {"error": f"Big Data Agent Failed: {str(e)}"}
    
# ==============================================================================
# 9. ML AGENT (Machine Learning Specialist)
# ==============================================================================
def ml_agent(payload, feedback=None):
    """
    Step 3/4: Generates a RiverGen Execution Plan for Machine Learning tasks.
    Handles predictions (regression/classification), forecasting, and anomaly detection.
    Supports Self-Correction Loop via 'feedback'.
    """
    print(f"🧠 [ML Agent] Generating plan... (Feedback Loop: {bool(feedback)})")

    # 1. Define Strict Output Template
    # Fits the 'rgen' spec for ML intent types
    response_template = {
        "request_id": payload.get("request_id"),
        "status": "success",
        "intent_type": "ml", # Explicitly ML intent
        "execution_plan": {
            "strategy": "hybrid_compute", # Often involves fetching data + inference
            "type": "ml_inference",
            "operations": [
                {
                    "step": 1,
                    "type": "model_inference",
                    "operation_type": "predict", # or 'train', 'forecast'
                    "model_details": {
                        "task_type": "classification", # e.g. regression, forecasting
                        "target_variable": "churn",    # inferred from prompt
                        "model_name": "propensity_to_churn_v2" # inferred or default
                    },
                    "input_data": {
                        "data_source_id": payload.get("data_sources", [{}])[0].get("data_source_id"),
                        "query": "SELECT * FROM ...", # Data fetching query
                        "features": ["age", "tenure", "usage"] # inferred features
                    },
                    "governance_applied": {
                        "masking_rules": []
                    }
                }
            ]
        }
    }

    # 2. Extract Governance & Schema Context
    data_sources = payload.get('data_sources', [])
    schema_summary = []

    for ds in data_sources:
        # We need to know if the source supports native ML (like BigQuery/Snowflake)
        source_type = ds.get('type', 'unknown')
        schema_summary.append(f"Source: {ds.get('name')} (Type: {source_type})")

        # Flatten tables to help LLM find features
        for schema in ds.get('schemas', []):
            for table in schema.get('tables', []):
                cols = [c['column_name'] for c in table.get('columns', [])]
                schema_summary.append(f" - Table '{table.get('table_name')}': {cols}")

    # 3. Build the Detailed System Prompt
    system_prompt = f"""
    You are the **ML Agent** for RiverGen AI.

    **YOUR TASK:**
    Generate an Execution Plan for a Machine Learning task (Prediction, Forecasting, or Classification).

    **INPUT CONTEXT:**
    - User Prompt: "{payload.get('user_prompt')}"
    - Data Sources: {json.dumps(schema_summary)}
    - User Context: {json.dumps(payload.get('user_context'))}

    **ML LOGIC RULES:**
    1. **Task Identification**: Analyze the prompt to determine the task.
       - "Predict churn" -> Classification (Target: churn/status)
       - "Forecast revenue" -> Time Series Forecasting (Target: revenue)
       - "Estimate LTV" -> Regression (Target: ltv_amount)

    2. **Feature Selection**: Look at the provided Schema (Tables & Columns). Select relevant columns to be used as `features` for the model.
       - *Example*: If predicting 'sales', select 'date', 'region', 'product_id'.

    3. **Compute Strategy**:
       - If the Data Source is **BigQuery** or **Snowflake**, prefer SQL-based ML syntax in the query (e.g., `ML.PREDICT` or `SELECT ... CALL!`).
       - If the Data Source is a **Database (Postgres)** or **File (S3)**, assume the data must be fetched first (`SELECT ...`) and passed to an external model service.

    4. **Governance**: Ensure PII (Personally Identifiable Information) like 'email' or 'ssn' is NOT used as a feature unless explicitly required and allowed by governance.

    **OUTPUT FORMAT:**
    Return ONLY valid JSON matching the exact template below. Fill in `model_details` and `input_data` fields intelligently.

    **OUTPUT TEMPLATE:**
    {json.dumps(response_template, indent=2)}
    """

    # 4. Inject Feedback (Self-Correction Logic)
    if feedback:
        system_prompt += f"""

        🚨 **CRITICAL: FIX PREVIOUS ERROR** 🚨
        Your previous plan was rejected by the QA Judge.
        **FEEDBACK:** "{feedback}"

        **INSTRUCTIONS FOR FIX:**
        - If the target variable was wrong, correct it.
        - If you selected columns that don't exist in the schema, remove them.
        - If the task type (e.g., 'classification' vs 'regression') was mismatched, fix it.
        """

    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME, # Uses global MODEL_NAME
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload)}
            ],
            temperature=0,
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)

    except Exception as e:
        return {"error": f"ML Agent Failed: {str(e)}"}