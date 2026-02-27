import logging

from fastapi import FastAPI

from app.core.logging_config import setup_logging
from app.core.config import config
from app.routers.execution import router as execution_router

# Configure logging before other app code runs
setup_logging(
    level=config.LOG_LEVEL or "INFO",
    log_file=config.LOG_FILE,
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="PSA Execution API",
    description="Accept execution payload and run intent, governance, and planning.",
    version="1.0.0",
)


@app.on_event("startup")
def on_startup():
    logger.info("PSA Execution API starting up")


@app.on_event("shutdown")
def on_shutdown():
    logger.info("PSA Execution API shutting down")


app.include_router(execution_router)
