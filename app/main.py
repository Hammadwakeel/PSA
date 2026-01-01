import time
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from app.routers import execution
from app.core.config import get_config # Assuming your config loader is here

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

# 4. ROUTERS
app.include_router(execution.router, prefix="/api/v1")

# 5. ENDPOINTS

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