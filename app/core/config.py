import os
from dotenv import load_dotenv
from groq import Groq

# Load environment variables from .env file
load_dotenv()

# --- API Keys & Secrets ---
# It is best practice to keep the key in a .env file. 
# Defaulting to your provided key for immediate functionality.
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# --- Model Configuration ---
# MODEL_NAME is set to the specific OSS model requested
MODEL_NAME = os.getenv("MODEL_NAME")

# --- Global Client Initialization ---
# Initializing the Groq client here allows it to be reused across agents.py
if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY must be set in environment variables or config.py")

client = Groq(api_key=GROQ_API_KEY)

# --- Runtime Constants ---
# Default execution constraints for the AI engine
DEFAULT_MAX_ROWS = 1000
DEFAULT_TIMEOUT = 30