from fastapi import APIRouter, HTTPException
from app.schemas.payload import ExecutionRequest
from app.services.rivergen import run_rivergen_flow

# Initialize the router with a specific tag for documentation
router = APIRouter(tags=["Execution"])

@router.post("/execute")
async def execute_prompt(request: ExecutionRequest):
    """
    Primary endpoint to process natural language prompts against data sources.
    
    Flow: 
    1. Validate input via Pydantic.
    2. Route to specialized agent (SQL, Stream, Vector, etc.).
    3. Generate and validate execution plan via Judge loop.
    """
    try:
        # Convert Pydantic model to a standard dictionary for the service layer
        payload = request.model_dump()
        
        # Trigger the RiverGen orchestration flow
        result = run_rivergen_flow(payload)
        
        # If the workflow returned an error status, raise a 400 or 500 exception
        if "error" in result or result.get("status") == "error":
            error_msg = result.get("error", "Unknown processing error")
            raise HTTPException(status_code=400, detail=error_msg)
            
        return result

    except Exception as e:
        # Catch unexpected failures and return a 500 Internal Server Error
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")