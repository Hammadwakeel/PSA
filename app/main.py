from fastapi import FastAPI
from app.routers import execution

# Initialize the FastAPI application
app = FastAPI(
    title="RiverGen AI Engine API",
    description="An intelligent orchestration API for routing and executing queries across diverse data sources.",
    version="1.0.0"
)

# 1. Health Check Endpoint
# Useful for monitoring tools or load balancers to verify service status
@app.get("/health", tags=["Monitoring"])
def health_check():
    """Returns the current status of the API."""
    return {
        "status": "healthy",
        "timestamp": "2025-12-31T19:42:00Z",  # Current system time
        "engine": "RiverGen-v1"
    }

# 2. Root Endpoint
@app.get("/", tags=["General"])
def read_root():
    """Welcome message for the API root."""
    return {
        "message": "Welcome to the RiverGen AI Engine. Access /docs for API documentation."
    }

# 3. Include Specialized Routers
# This brings in the /execute endpoint from app/routers/execution.py
app.include_router(execution.router, prefix="/api/v1")