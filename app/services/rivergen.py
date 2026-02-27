import logging

from app.services.rivergen_backend import RgenClient
from app.services.agents import json_agent, multiflow_planning
from app.schemas.agent_output import DecideIntent, GovernanceApply
from app.schemas.intents import PSA_INTENTS, PSA_INTENTS_TOOLS
from app.schemas.prompt import get_intent_prompt, get_governance_prompt, get_planning_prompt
from app.core.config import config

logger = logging.getLogger(__name__)

client = RgenClient(
    email=config.RGEN_EMAIL,
    password=config.RGEN_PASSWORD,
    verify=config.RGEN_VERIFY not in ("false", "0", "False"),
)

def execute_tool_real(client: RgenClient, tool_name: str, **kwargs):
    """Execute a backend tool by name. Used by the planning agent."""
    return client.execute_tool_real(tool_name, **kwargs)


def _tools_list_for_subintent(client: RgenClient, sub_intent_config: dict):
    """Build Gemini function_declarations list from subintent config."""
    raw = sub_intent_config.get("tools", [])
    names = [t["name"] for t in raw if isinstance(t, dict)] if raw and isinstance(raw[0], dict) else raw
    all_declarations = client.get_tools()
    selected = [d for d in all_declarations if d["name"] in names]
    logger.debug("_tools_list_for_subintent | names=%s | selected=%s", names, [d["name"] for d in selected])
    return selected


def run(request: dict):
    logger.info("run started | user_prompt_len=%s", len(request.get("user_prompt", "")))
    intent_prompt = get_intent_prompt(
        request.get("user_prompt", ""),
        request.get("user_context", {}),
        PSA_INTENTS,
    )
    intent_response = json_agent(intent_prompt, DecideIntent)
    logger.info(
        "intent_response | detect_psa_intent=%s | psa_intent=%s | psa_sub_intent=%s",
        intent_response.get("detect_psa_intent"),
        intent_response.get("psa_intent"),
        intent_response.get("psa_sub_intent"),
    )
    if not intent_response.get("detect_psa_intent"):
        logger.warning("run finished early: no intent detected")
        return {"final_message": "No intent found", "steps": []}

    data_sources = request.get("data_sources", [])
    if not data_sources:
        logger.warning("run finished early: no data_sources in request")
        return {"final_message": "No data sources in request", "steps": []}
    ds0 = data_sources[0]
    data_source_schema = (
        f"name: {ds0.get('name')}, type: {ds0.get('type')}, schema: {ds0.get('schemas')}"
    )
    available_governance_policies = request.get("available_governance_policies") or ds0.get("governance_policies", {})

    logger.info("running governance check | data_source=%s", ds0.get("name"))
    governance_prompt = get_governance_prompt(
        request.get("user_prompt", ""),
        request.get("user_context", {}),
        data_source_schema,
        available_governance_policies,
    )
    json_agent(governance_prompt, GovernanceApply)
    logger.debug("governance check completed")

    psa_sub_intent = intent_response.get("psa_sub_intent")
    sub_intent_config = PSA_INTENTS_TOOLS.get(psa_sub_intent)
    if not sub_intent_config:
        logger.warning("run finished early: no tool config for sub_intent=%s", psa_sub_intent)
        return {"final_message": f"No tool config for sub-intent: {psa_sub_intent}", "steps": []}

    tools_list = _tools_list_for_subintent(client, sub_intent_config)
    if not tools_list:
        logger.warning("run finished early: no tools for sub_intent=%s", psa_sub_intent)
        return {"final_message": f"No tools found for sub-intent: {psa_sub_intent}", "steps": []}

    data_source_id = ds0.get("data_source_id")
    if data_source_id is None:
        logger.warning("data_source_id missing on first data source; planning may fail for tools that require it")
    flow_mode = "multi" if sub_intent_config.get("sequence_required") else "single"
    logger.info(
        "starting multiflow_planning | sub_intent=%s | flow_mode=%s | tools=%s | data_source_id=%s",
        psa_sub_intent,
        flow_mode,
        [t["name"] for t in tools_list],
        data_source_id,
    )
    planning_prompt = get_planning_prompt(
        psa_sub_intent,
        sub_intent_config,
        request.get("user_prompt", ""),
        data_source_id=data_source_id or 0,
    )
    result = multiflow_planning(client, planning_prompt, tools_list, flow_mode=flow_mode)
    logger.info(
        "run completed | final_message_len=%s | steps_count=%s",
        len(str(result.get("final_message", ""))),
        len(result.get("steps", [])),
    )
    return result