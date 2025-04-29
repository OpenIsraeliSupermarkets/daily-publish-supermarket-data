import asyncio
import logging
import os
import sys

# Add parent directory to Python path so we can import modules from it
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_processing_validation import validate_data_processing
from data_serving_validation import main as full_data_scan
from static_validation import validate_data_storage
from il_supermarket_scarper import ScraperFactory
from utils import now

async def run_validations():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Run data processing validation
    # Run both validations concurrently
    tasks = []

    logging.info("Starting data processing validation...")

    tasks.append(
        asyncio.create_task(
            asyncio.to_thread(validate_data_processing, uri=os.getenv("MONGODB_URI"))
        )
    )

    # # Data serving validation task
    # tasks.append(
    #     asyncio.create_task(
    #         full_data_scan(
    #             os.getenv("API_TOKEN"),
    #             os.getenv("API_HOST"),
    #             int(os.getenv("RATE_LIMIT", "3")),
    #         )
    #     )
    # )

    # Data kaggle validation task
    tasks.append(
        asyncio.create_task(
            asyncio.to_thread(
                validate_data_storage,
                os.getenv("KAGGLE_DATASET_REMOTE_NAME"),
                os.getenv("ENABLED_SCRAPERS",",".join(ScraperFactory.all_scrapers_name())).split(","),
                os.getenv("MONGODB_URI"),
                file_per_run=int(os.getenv("LIMIT")) if os.getenv("LIMIT") else None,
                num_of_occasions=int(os.getenv("NUM_OF_OCCASIONS")) if os.getenv("NUM_OF_OCCASIONS") else None,
                upload_to_long_term_db=True # assume running after the publish
            )
        )
    )

    # Wait for both tasks to complete
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(run_validations())
