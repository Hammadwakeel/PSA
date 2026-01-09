import time
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from app.routers import execution
from app.core.config import get_config
from app.schemas.payload import ExecutionResponse, ErrorDetail

# Setup logging
logger = logging.getLogger("rivergen_api")

# 1. LIFESPAN MANAGER (The "Warm-Up" Phase)
# Replaces the deprecated @app.on_event("startup")
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Execute setup logic before the API starts accepting requests.
    """
    config = get_config()
    print(f"🚀 [Startup] RiverGen AI Engine ({config.MODEL_NAME}) is warming up...")
    
    # Optional: Pre-initialize heavy objects here (Database pools, LLM clients)
    # from app.core.config import get_groq_client
    # get_groq_client() 
    
    yield  # API is running now
    
    print("🛑 [Shutdown] Cleaning up resources...")

# 2. INITIALIZE APP
app = FastAPI(
    title="RiverGen AI Engine API",
    description="Enterprise orchestration API for executing queries across SQL, NoSQL, and Streaming sources.",
    version="1.0.0",
    lifespan=lifespan, # Attach startup logic
    docs_url="/docs",
    redoc_url="/redoc"
)

# 3. MIDDLEWARE (Security & Tracing)

# A. CORS (Allow Frontend Access)
origins = [
    "http://localhost:3000",      # React Localhost
    "https://app.rivergen.ai",    # Production Frontend
    "https://staging.rivergen.ai" # Staging
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, # Restrict this in Prod! Don't use ["*"]
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# B. Request ID & Timing Middleware
# Adds X-Process-Time header and ensures logs can be traced
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# 4. EXCEPTION HANDLERS (Structured Error Responses)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Handle FastAPI request validation errors (incorrect request body, missing fields, etc.)
    Returns structured error response format.
    """
    # Extract request_id - for validation errors, the request body is invalid
    # so we can't reliably extract from it. Use query params or default to "unknown"
    request_id = request.query_params.get("request_id", "unknown")
    execution_id = request.query_params.get("execution_id")
    
    # Format validation errors into readable messages
    errors = exc.errors()
    error_messages = []
    for error in errors:
        field = " -> ".join(str(loc) for loc in error.get("loc", []))
        msg = error.get("msg", "Validation error")
        error_messages.append(f"{field}: {msg}")
    
    error_message = "; ".join(error_messages) if error_messages else "Request validation failed"
    
    # Determine error code based on validation error type
    error_code = "VALIDATION_ERROR"
    if any("field required" in str(err.get("msg", "")).lower() for err in errors):
        error_code = "MISSING_REQUIRED_FIELD"
    elif any("value is not a valid" in str(err.get("msg", "")).lower() for err in errors):
        error_code = "INVALID_FIELD_VALUE"
    
    error_detail = ErrorDetail(
        code=error_code,
        message=f"Request validation failed: {error_message}",
        details={
            "validation_errors": errors,
            "path": request.url.path
        },
        suggested_fixes=[
            "Review the request body structure and required fields",
            "Check field types match the expected schema",
            "Ensure all required fields are provided",
            "Refer to API documentation for correct request format"
        ]
    )
    
    error_response = ExecutionResponse(
        request_id=request_id,
        execution_id=execution_id,
        status="error",
        timestamp=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        error=error_detail
    )
    
    logger.warning(f"⚠️ [Validation Error] {request_id}: {error_message}")
    
    return JSONResponse(
        content=error_response.model_dump(exclude_none=True),
        status_code=status.HTTP_200_OK
    )

@app.exception_handler(ValidationError)
async def pydantic_validation_exception_handler(request: Request, exc: ValidationError):
    """
    Handle Pydantic validation errors.
    """
    request_id = "unknown"
    # For Pydantic validation errors, body might not be accessible
    # Skip trying to read it to avoid additional errors
    
    error_detail = ErrorDetail(
        code="VALIDATION_ERROR",
        message="Data validation failed",
        details={
            "errors": str(exc.errors())
        },
        suggested_fixes=[
            "Review the data structure and ensure it matches the expected schema"
        ]
    )
    
    error_response = ExecutionResponse(
        request_id=request_id,
        status="error",
        timestamp=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        error=error_detail
    )
    
    return JSONResponse(
        content=error_response.model_dump(exclude_none=True),
        status_code=status.HTTP_200_OK
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """
    Handle HTTPException (including custom ones that might not have structured format).
    Returns structured error response format.
    """
    request_id = "unknown"
    execution_id = None
    
    # For HTTPException, try to extract request_id from detail if it's a dict
    detail = exc.detail
    if isinstance(detail, dict):
        request_id = detail.get("request_id", "unknown")
        execution_id = detail.get("execution_id")
        
        # Check if detail is already a structured error response
        if "error" in detail and isinstance(detail["error"], dict):
            # Already structured, return as-is with status 200
            return JSONResponse(
                content=detail,
                status_code=status.HTTP_200_OK
            )
    elif request.url.path.endswith("/execute") and request.method == "POST":
        # Try to read body as last resort, but don't fail if it's invalid
        try:
            body = await request.json()
            request_id = body.get("request_id", "unknown")
            execution_id = body.get("execution_id")
        except:
            pass
    
    # Convert to structured format
    error_detail = ErrorDetail(
        code=f"HTTP_{exc.status_code}",
        message=str(detail) if detail else "An error occurred",
        details={
            "status_code": exc.status_code,
            "path": request.url.path
        },
        suggested_fixes=[
            "Review the request and ensure it's valid",
            "Check API documentation for correct usage",
            "Contact support if the issue persists"
        ]
    )
    
    error_response = ExecutionResponse(
        request_id=request_id,
        execution_id=execution_id,
        status="error",
        timestamp=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        error=error_detail
    )
    
    logger.warning(f"⚠️ [HTTP Exception] {request_id}: {exc.status_code} - {detail}")
    
    return JSONResponse(
        content=error_response.model_dump(exclude_none=True),
        status_code=status.HTTP_200_OK
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """
    Global exception handler for any unhandled exceptions.
    Returns structured error response format.
    """
    request_id = "unknown"
    execution_id = None
    
    # For general exceptions, try to read body if possible
    # But don't fail if body is invalid/corrupted
    if request.url.path.endswith("/execute") and request.method == "POST":
        try:
            # Only try if request has a body
            if request.headers.get("content-length") or request.headers.get("content-type"):
                body = await request.json()
                if isinstance(body, dict):
                    request_id = body.get("request_id", "unknown")
                    execution_id = body.get("execution_id")
        except:
            # If body can't be read, just use default values
            pass
    
    # Log the full error internally
    logger.error(f"❌ [Unhandled Exception] {request_id}: {str(exc)}", exc_info=True)
    
    error_detail = ErrorDetail(
        code="INTERNAL_SERVER_ERROR",
        message="An unexpected error occurred. Please contact support.",
        details={
            "request_id": request_id,
            "error_type": type(exc).__name__,
            "path": request.url.path
        },
        suggested_fixes=[
            f"Contact support with Request ID: {request_id}",
            "Check API logs for detailed error information",
            "Verify the request format matches API documentation"
        ]
    )
    
    error_response = ExecutionResponse(
        request_id=request_id,
        execution_id=execution_id,
        status="error",
        timestamp=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        error=error_detail
    )
    
    return JSONResponse(
        content=error_response.model_dump(exclude_none=True),
        status_code=status.HTTP_200_OK
    )

# 5. ROUTERS
app.include_router(execution.router, prefix="/api/v1")

# 6. ENDPOINTS

@app.get("/health", tags=["Monitoring"])
def health_check():
    """
    Dynamic health check for load balancers.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(), # ✅ FIXED: Dynamic time
        "engine": "RiverGen-v1",
        "uptime_check": True
    }

@app.get("/", tags=["General"])
def read_root():
    return {
        "message": "RiverGen AI Engine is running.",
        "docs": "/docs",
        "health": "/health"
    }

if __name__ == "__main__":
    import uvicorn
    # In production, you usually run this via: uvicorn main:app --workers 4
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)