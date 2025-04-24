import asyncio
import logging
import traceback
import json
import importlib.util
import sys
import os

def check_module_available(module_name):
    """Check if a module is available without importing it directly."""
    return importlib.util.find_spec(module_name) is not None

async def run_validations():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
# Run data processing validation
    try:
        logging.info("Starting data processing validation...")
        from data_processing_validation import collect_validation_results
        collect_validation_results(uri="mongodb://your_mongo_user:your_mongo_password@localhost:27017/")
    except Exception as e:
        logging.error(f"Error in data processing validation: {str(e)}")
        logging.error(traceback.format_exc())

    # Run data serving validation
    try:
        api_token = os.getenv("API_TOKEN","0f8698d8-db8f-46e7-b460-8e0a2f3abab9")
        host = os.getenv("API_HOST", "http://localhost:8080")
        rate_limit = int(os.getenv("RATE_LIMIT", "3")) 
        from data_serving_validation import main
        await main(api_token,host,rate_limit)
    except Exception as e:
        logging.error(f"Error in data serving validation: {str(e)}")
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(run_validations())

