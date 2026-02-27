import logging

from fastapi import APIRouter, HTTPException

from app.schemas.payload import ExecutionRequest, ExecutionResponse
from app.services.rivergen import run

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/execute", tags=["execution"])


@router.post("", response_model=ExecutionResponse)
def execute(request: ExecutionRequest):
    """
    Accepts an execution payload, runs intent + governance + planning.
    Returns final_message and steps; each step has response_text (model's previous
    message that triggered the tool calls), tool_calls (name, args, output).
    """
    request_id = getattr(request, "request_id", None) or "(no id)"
    user_prompt_preview = (request.user_prompt or "")[:80]
    logger.info(
        "execute request received | request_id=%s | user_prompt_preview=%s | data_sources_count=%s",
        request_id,
        user_prompt_preview,
        len(request.data_sources or []),
    )
    try:
        result = run(request.to_request_dict())
        if not isinstance(result, dict):
            result = {"final_message": str(result), "steps": []}
        logger.info(
            "execute request completed | request_id=%s | steps_count=%s",
            request_id,
            len(result.get("steps", [])),
        )
        return ExecutionResponse(**result)
    except Exception as e:
        logger.exception(
            "execute request failed | request_id=%s | error=%s",
            request_id,
            str(e),
        )
        raise HTTPException(status_code=500, detail=str(e))
