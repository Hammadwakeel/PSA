import os
import logging
from functools import lru_cache
from dotenv import load_dotenv
from groq import Groq

# 1. Setup Logging (Essential for Prod)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 2. Load .env only if strictly necessary (Dev mode)
# In Prod, we expect vars to be set by the orchestrator (K8s/Docker)
load_dotenv() 

class AppConfig:
    """
    Centralized Configuration Management.
    """
    def __init__(self):
        # --- API Keys & Secrets ---
        self.GROQ_API_KEY = os.getenv("GROQ_API_KEY")
        
        # --- Model Configuration ---
        self.MODEL_NAME = os.getenv("MODEL_NAME", "meta-llama/llama-3.3-70b-versatile")  # Good practice: Have a fallback

        # --- Runtime Constants (Tunable via Env) ---
        self.DEFAULT_MAX_ROWS = int(os.getenv("DEFAULT_MAX_ROWS", 1000))
        self.DEFAULT_TIMEOUT = int(os.getenv("DEFAULT_TIMEOUT", 30))

        self.validate()

    def validate(self):
        """Fail fast if critical config is missing."""
        if not self.GROQ_API_KEY:
            # Log error before crashing so it appears in CloudWatch/Datadog
            logger.critical("❌ GROQ_API_KEY is missing from environment variables.")
            raise ValueError("GROQ_API_KEY must be set.")
        
        if not self.MODEL_NAME:
            logger.warning("⚠️ MODEL_NAME not set. Using default.")

# 3. Lazy Loading Pattern (The Fix)
@lru_cache()
def get_config():
    """
    Creates the config object once and caches it.
    """
    return AppConfig()

@lru_cache()
def get_groq_client():
    """
    Initializes the Groq client ONLY when first called.
    Prevents 'import time' crashes.
    """
    config = get_config()
    try:
        client = Groq(api_key=config.GROQ_API_KEY)
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Groq Client: {e}")
        raise
