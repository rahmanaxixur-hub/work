import os
from dotenv import load_dotenv

# Automatically load environment variables from .env file at project startup
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
