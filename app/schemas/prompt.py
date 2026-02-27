import json
from typing import Dict, List


def get_intent_prompt(user_prompt: str, user_context: dict, intent_map: Dict[str, List[str]]):
    """
    Generates a prompt with dynamic numbered mapping,
    including ONLY user context and user prompt.
    """

    # Generate Numbered Taxonomy string dynamically
    taxonomy_lines = []
    for i, (main_intent, sub_intents) in enumerate(intent_map.items(), 1):
        taxonomy_lines.append(f"{i}. MAIN INTENT: {main_intent}")
        for j, sub in enumerate(sub_intents, 1):
            taxonomy_lines.append(f"   {i}.{j} SUB-INTENT: {sub}")

    intent_taxonomy_str = "\n".join(taxonomy_lines)

    # Prepare Context (Only user_context as requested)
    context_str = json.dumps(user_context, indent=2)

    prompt = f"""
### ROLE
You are an expert Intent Classification Agent. Your goal is to map a user request to a specific PSA intent and sub-intent based ONLY on the provided taxonomy and user context.

### INTENT TAXONOMY (Numbered Mapping)
{intent_taxonomy_str}

### INPUT DATA
**User Context**:
{context_str}

**User Prompt**:
"{user_prompt}"

### INSTRUCTIONS
1. **Analyze**: Review the User Prompt against the User Context (Roles/Permissions) and the Intent Taxonomy.
2. **Match**: If a clear match exists in the numbered list above, set `detect_psa_intent` to `True` and provide the names of the intent and sub-intent.
3. **Fallback**: If the prompt is irrelevant, ambiguous, or outside the numbered taxonomy, set `detect_psa_intent` to `False` and return empty strings for the intent keys.
4. **Output**: Return a JSON object matching the requested schema.

### TASK
Think step-by-step. Is the user asking for one of the {len(intent_map)} main intents listed above?
Identify the correct Intent and Sub-intent names.


Generate your output in Json Output Format:
{{detect_psa_intent: boolean value if detect intent make it true otherwise false,
psa_intent: str value intent name if detect write name if not detected add none,
psa_sub_intent: str value sub intent name if detect write name if not detected add none}}
"""
    return prompt

def get_governance_prompt(user_prompt: str, user_context: dict, data_source_schema, policies_str):
    context_str = json.dumps(user_context, indent=2)
    return f"""
### ROLE
You are a Senior Data Governance & Security Auditor. Your task is to evaluate a User Prompt against specific Security Policies and User Context.

### SECURITY PILLARS DEFINITION
1. **RBAC (Role Based Access Control)**: Check if 'roles' or 'permissions' in User Context allow access to the requested data.
2. **Row-Level-Security (RLS)**: Look for 'row_level_security' in the data source. If 'enabled' is true, identify the 'rules' (e.g., region filters).
3. **Data Masking (DM)**: Look for 'column_masking' in the data source. If 'enabled' is true, identify which columns (like PII) must be hidden.
4. **Query Limits (QM)**: Check for constraints like 'max_rows' or 'timeout_seconds' in the execution context.

### INPUT DATA
- **User Prompt**: "{user_prompt}"
- **User Context**: {context_str}
- **Data Source Schema**: {data_source_schema}
- **Available Governance Policies**: {policies_str}
In Available Governance Policies if RBAC is missin it should be false if present but enabled is false then it should be false make sure that remember this while generating an output.
In Available Governance Policies if Row-Level-Security is missin it should be false if present but enabled is false then it should be false make sure that remember this while generating an output.
In Available Governance Policies if Data Masking is missin it should be false if present but enabled is false then it should be false make sure that remember this while generating an output.
In Available Governance Policies if Query Limits is missin it should be false if present but enabled is false then it should be false make sure that remember this while generating an output.


### INSTRUCTIONS & LOGIC
- **Check Availability**: For each pillar, set `available` to `true` ONLY if the policy is found in the input data and is marked as `enabled: true`.
- **Handling Disabled Policies**: If a policy is missing, or marked as `enabled: false`, you MUST set `available: false` and `explanation: null`.
- **Explanation Requirements**: If `available: true`, provide a detailed explanation of the specific rule being applied (e.g., "Filtering by region US-WEST").

### OUTPUT FORMAT
You must return a JSON object with this exact structure:
{{
  "RBAC": {{ "explanation": "str or null", "available": boolean }},
  "Row-Level-Security": {{ "explanation": "str or null", "available": boolean }},
  "Data Masking": {{ "explanation": "str or null", "available": boolean }},
  "Query Limits": {{ "explanation": "str or null", "available": boolean }}
}}
"""

def get_planning_prompt(
    sub_intent: str,
    config: dict,
    user_prompt: str,
    data_source_id: int,
):
    """
    Build the planning prompt. data_source_id is passed from the request so the agent
    uses it when calling get_data_source, discover_schema, test_connection, get_schemas.
    """
    data_source_ctx = f"Use data_source_id={data_source_id} for all tool calls that require a data_source_id parameter."
    if config.get("sequence_required"):
        order_str = "\n".join([f"Step {t['order']}: {t['name']}" for t in config['tools']])
        return f"""You are a Sequential Agent. Task: {user_prompt}.

{data_source_ctx}

STRICT ORDER (you must follow this order; one tool per turn):
{order_str}

RULES:
1. Call exactly ONE tool per turn, in the order above. Do not skip steps.
2. If a tool requires data_source_id, use data_source_id={data_source_id}.
3. If a tool has already succeeded, do not call it again; call the next tool in the order.
4. CRITICAL - When a tool FAILS (you receive an error in the tool output):
   - First try to CORRECT it: call the SAME tool again with corrected parameters if the error can be fixed (e.g. wrong ID, invalid argument).
   - If you cannot fix the error (e.g. "not found", "permission denied"), then STOP. Do NOT call the next tool in the sequence. Return a clear message to the user explaining the failure and that the pipeline stopped.
   - Never call the next tool in the order until the current tool has succeeded.
5. If the current step cannot be fixed after a retry, stop and report the error. Do not proceed to the next step.
"""
    else:
        return f"""You are an expert Agent. Task: {user_prompt}.

{data_source_ctx}

Available tools: {config['tools']}.
Order is not required; you can call one or multiple tools based on the user prompt.
When a tool requires data_source_id, use data_source_id={data_source_id}.
"""
