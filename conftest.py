"""
Pytest configuration file that loads environment variables from .env.unittest
"""

from pathlib import Path

# Load environment variables from .env.unittest if it exists
env_file = Path(__file__).parent / ".env.unittest"
if env_file.exists():
    from dotenv import load_dotenv

    load_dotenv(env_file)
