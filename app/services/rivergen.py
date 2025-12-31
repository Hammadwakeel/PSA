import time
from app.core.agents import (
    router_agent, sql_agent, nosql_agent, multi_source_agent, 
    big_data_agent, ml_agent, vector_store_agent, stream_agent, llm_judge
)

def run_rivergen_flow(payload):
    """
    Main workflow orchestrator that manages the Routing -> Execution -> Judging loop.
    """
    print(f"\n{' STARTING RIVERGEN FLOW ':=^60}")

    # --- Step 1 & 2: Router Agent ---
    router_output = router_agent(payload)

    if "error" in router_output:
        return {"error": router_output["error"]}

    agent_name = router_output.get("selected_agent")
    confidence = router_output.get("confidence", "N/A")
    print(f"🧭 [Router] Selected: {agent_name} (Confidence: {confidence})")
    print(f"   Reasoning: {router_output.get('reasoning')}")

    # --- Agent Dispatcher ---
    # Mapping the router's string output to actual function references
    agent_mapping = {
        "sql_agent": sql_agent,
        "nosql_agent": nosql_agent,
        "multi_source_agent": multi_source_agent,
        "big_data_agent": big_data_agent,
        "ml_agent": ml_agent,
        "vector_store_agent": vector_store_agent,
        "stream_agent": stream_agent
    }

    agent_func = agent_mapping.get(agent_name)

    if not agent_func:
        return {"error": f"Agent '{agent_name}' is not defined or supported."}

    print(f"👉 [Dispatcher] Handing off to: {agent_name.upper()}")

    # --- Step 3, 4, 5: Generation & Validation Loop ---
    max_retries = 3
    current_feedback = None

    for attempt in range(max_retries):
        print(f"\n--- Attempt {attempt + 1}/{max_retries} ---")

        try:
            # Step 3 & 4: Generate Plan with optional feedback for self-correction
            plan = agent_func(payload, feedback=current_feedback)
            
            if "error" in plan:
                print(f"⚠️ [Agent Error]: {plan['error']}")
                current_feedback = f"Agent crashed with error: {plan['error']}"
                continue

            # Step 5: LLM Judge Validation
            review = llm_judge(payload, plan)

            if review.get('approved'):
                print(f"✅ [Judge] Plan Approved!")
                return plan 

            else:
                print(f"❌ [Judge] Rejected.")
                print(f"   Feedback: {review.get('feedback')}")
                current_feedback = review.get('feedback')
                print("🔄 Looping back to Agent for correction...")
                time.sleep(1) 

        except Exception as e:
             return {"error": f"CRITICAL: Workflow failed on {agent_name}: {str(e)}"}

    return {
        "status": "error",
        "error": "Plan generation failed after max retries.",
        "last_feedback": current_feedback
    }