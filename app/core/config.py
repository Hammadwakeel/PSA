import logging
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class Config:
    RGEN_EMAIL = os.getenv("RGEN_EMAIL")
    RGEN_PASSWORD = os.getenv("RGEN_PASSWORD")
    RGEN_VERIFY = os.getenv("RGEN_VERIFY")
    GENAI_MODEL = os.getenv("GENAI_MODEL")
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "").strip() or None


config = Config()

# Log which config keys are set (values never logged)
def _config_status():
    keys = ["RGEN_EMAIL", "RGEN_PASSWORD", "RGEN_VERIFY", "GENAI_MODEL", "GOOGLE_API_KEY", "LOG_LEVEL", "LOG_FILE"]
    status = {k: "set" if getattr(config, k, None) else "unset" for k in keys}
    logger.debug("config loaded | %s", status)


_config_status()
