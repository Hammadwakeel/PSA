import logging
from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.concurrency import run_in_threadpool # 👈 Key for fixing blocking calls
from app.schemas.payload import ExecutionRequest, ExecutionResponse # Assuming you have a Response schema
from app.services.rivergen import run_rivergen_flow

# 1. Setup Structured Logging
logger = logging.getLogger("api_execution")

router = APIRouter(tags=["Execution"])

@router.post(
    "/execute", 
    response_model=dict, # Better: Replace 'dict' with actual Pydantic schema 'ExecutionResponse'
    summary="Execute AI Flow",
    description="Processes natural language prompts via the RiverGen Engine."
)
async def execute_prompt(request: ExecutionRequest):
    """
    Primary endpoint to process natural language prompts against data sources.
    Uses threadpooling to prevent blocking the async event loop.
    """
    request_id = request.request_id or "unknown"
    logger.info(f"🚀 [API] Received execution request: {request_id}")

    try:
        # Convert Pydantic model to dict
        payload = request.model_dump()
        
        # ------------------------------------------------------------------
        # ⚡ CRITICAL FIX: Run Blocking Code in Threadpool
        # ------------------------------------------------------------------
        # Since 'run_rivergen_flow' is CPU/IO bound and synchronous,
        # we offload it to a worker thread so the main Async loop stays alive.
        result = await run_in_threadpool(run_rivergen_flow, payload)
        
        # Check logical errors from the service layer
        if result.get("status") == "error" or "error" in result:
            error_msg = result.get("error", "Unknown processing error")
            logger.warning(f"⚠️ [API] Logic Error for {request_id}: {error_msg}")
            raise HTTPException(status_code=400, detail=error_msg)
            
        logger.info(f"✅ [API] Success for {request_id}")
        return result

    except HTTPException:
        # Re-raise HTTP exceptions so they propagate correctly
        raise

    except Exception as e:
        # 🔒 SECURITY FIX: Log the real error, hide it from user
        logger.error(f"❌ [API] System Crash for {request_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail="Internal Server Error. Please contact support with Request ID."
        )