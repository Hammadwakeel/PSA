import time
import logging
from typing import Dict, Any, Optional

# Import agents
from app.core.agents import (
    router_agent, sql_agent, nosql_agent, multi_source_agent, 
    big_data_agent, ml_agent, vector_store_agent, stream_agent, llm_judge
)

# 1. Setup Structured Logging
# This ensures logs appear in Datadog/CloudWatch with the correct severity
logger = logging.getLogger("rivergen.orchestrator")
logging.basicConfig(level=logging.INFO)

# 2. Agent Registry (Constant)
# Defined globally to avoid recreating it on every request
AGENT_MAPPING = {
    "sql_agent": sql_agent,
    "nosql_agent": nosql_agent,
    "multi_source_agent": multi_source_agent,
    "big_data_agent": big_data_agent,
    "ml_agent": ml_agent,
    "vector_store_agent": vector_store_agent,
    "stream_agent": stream_agent
}

def run_rivergen_flow(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main workflow orchestrator: Routing -> Execution -> Judging Loop.
    Hardened for production observability and error resilience.
    """
    request_id = payload.get("request_id", "unknown_id")
    start_time = time.time()
    
    logger.info(f"🚀 [Orchestrator] Starting Flow for Request ID: {request_id}")

    try:
        # --- Step 1: Router Agent ---
        router_output = router_agent(payload)

        if "error" in router_output:
            logger.error(f"⛔ [Router Error] {request_id}: {router_output['error']}")
            return {"status": "error", "error": router_output["error"]}

        agent_name = router_output.get("selected_agent")
        confidence = router_output.get("confidence", 0.0)
        
        logger.info(f"🧭 [Router] {request_id} -> Selected: {agent_name} (Conf: {confidence})")

        # --- Step 2: Agent Dispatch ---
        agent_func = AGENT_MAPPING.get(agent_name)
        if not agent_func:
            error_msg = f"Agent '{agent_name}' is not supported."
            logger.critical(f"❌ [Dispatcher] {error_msg}")
            return {"status": "error", "error": error_msg}

        # --- Step 3-5: Generation & Validation Loop ---
        max_retries = 3
        current_feedback = None

        for attempt in range(1, max_retries + 1):
            logger.info(f"🔄 [Attempt {attempt}/{max_retries}] Agent '{agent_name}' working...")

            # A. Generate Plan
            plan = agent_func(payload, feedback=current_feedback)
            
            # Check for Agent Crash
            if plan.get("error"):
                logger.warning(f"⚠️ [Agent Crash] Attempt {attempt} failed: {plan['error']}")
                current_feedback = f"Agent crashed with error: {plan['error']}"
                continue

            # B. Validate Plan (Judge)
            review = llm_judge(payload, plan)

            if review.get('approved'):
                duration = time.time() - start_time
                logger.info(f"✅ [Judge] Plan Approved for {request_id} in {duration:.2f}s")
                
                # C. Inject Execution Metadata
                plan["meta"] = {
                    "attempts_used": attempt,
                    "processing_time_ms": int(duration * 1000),
                    "router_confidence": confidence,
                    "judge_score": review.get("score", 1.0)
                }
                return plan

            else:
                feedback = review.get('feedback', 'Unknown rejection reason.')
                logger.info(f"❌ [Judge] Rejected attempt {attempt}. Feedback: {feedback}")
                current_feedback = feedback
                
                # Optional: Add small backoff if hitting rate limits
                # time.sleep(0.5)

        # --- Final Failure State ---
        logger.error(f"💀 [Failed] {request_id} exhausted {max_retries} attempts.")
        return {
            "status": "error",
            "error": "Plan generation failed validation after maximum retries.",
            "last_feedback": current_feedback,
            "request_id": request_id
        }

    except Exception as e:
        # Catch-all for unexpected crashes (e.g., memory errors, timeouts)
        logger.exception(f"🔥 [System Panic] Critical workflow failure for {request_id}")
        return {
            "status": "error", 
            "error": "Internal Orchestration Error. Please check logs.",
            "details": str(e),
            "request_id": request_id
        }