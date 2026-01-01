import logging
from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool
from app.schemas.payload import ExecutionRequest  # Ensure this import matches your project structure
from app.services.rivergen import run_rivergen_flow

# 1. Setup Structured Logging
logger = logging.getLogger("api_execution")

router = APIRouter(tags=["Execution"])

@router.post(
    "/execute", 
    response_model=dict,  # Ideally, use a strict Pydantic model here if available
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
        # Since 'run_rivergen_flow' is synchronous, we offload it to a worker thread.
        result = await run_in_threadpool(run_rivergen_flow, payload)
        
        # Check logical errors from the service layer
        if result.get("status") == "error" or "error" in result:
            error_msg = result.get("error", "Unknown processing error")
            
            # 🛠️ IMPROVEMENT: Extract detailed Judge feedback if available
            last_feedback = result.get("last_feedback", "")
            if last_feedback:
                detailed_detail = f"{error_msg} \n\n🛑 REASON: {last_feedback}"
            else:
                detailed_detail = error_msg

            logger.warning(f"⚠️ [API] Logic Error for {request_id}: {error_msg}")
            
            # Return 400 Bad Request with the detailed reason
            raise HTTPException(status_code=400, detail=detailed_detail)
            
        logger.info(f"✅ [API] Success for {request_id}")
        return result

    except HTTPException:
        # Re-raise known HTTP exceptions so they propagate correctly
        raise

    except Exception as e:
        # 🔒 SECURITY FIX: Log the real error internally, hide raw traceback from user
        logger.error(f"❌ [API] System Crash for {request_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"Internal Server Error. Please contact support with Request ID: {request_id}"
        )