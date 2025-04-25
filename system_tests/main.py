import asyncio
import logging
import traceback
import json
import sys
import os


async def run_validations():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
# Run data processing validation
    # Run both validations concurrently
    tasks = []
    
    # Data processing validation task
    try:
        logging.info("Starting data processing validation...")
        from data_processing_validation import collect_validation_results
        tasks.append(asyncio.create_task(
            asyncio.to_thread(collect_validation_results, 
            uri=os.getenv("MONGODB_URI"))
        ))
    except Exception as e:
        logging.error(f"Error in data processing validation: {str(e)}")
        logging.error(traceback.format_exc())

    # Data serving validation task  
    try:
        api_token = os.getenv("API_TOKEN")
        host = os.getenv("API_HOST")
        rate_limit = int(os.getenv("RATE_LIMIT", "3"))
        from data_serving_validation import main
        tasks.append(asyncio.create_task(main(api_token,host,rate_limit)))
    except Exception as e:
        logging.error(f"Error in data serving validation: {str(e)}")
        logging.error(traceback.format_exc())

    # Wait for both tasks to complete
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(run_validations())

