import asyncio
import logging
import os
from data_processing_validation import collect_validation_results
from data_serving_validation import main
from data_kaggle_validation import download_and_validate_kaggle_data


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
            asyncio.to_thread(collect_validation_results, uri=os.getenv("MONGODB_URI"))
        )
    )

    # Data serving validation task
    tasks.append(
        asyncio.create_task(
            main(
                os.getenv("API_TOKEN"),
                os.getenv("API_HOST"),
                int(os.getenv("RATE_LIMIT", "3")),
            )
        )
    )

    # Data kaggle validation task
    tasks.append(
        asyncio.create_task(
            download_and_validate_kaggle_data(
                os.getenv("KAGGLE_DATASET_NAME"),
                os.getenv("ENABLED_SCRAPERS").split(","),
            )
        )
    )

    # Wait for both tasks to complete
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(run_validations())
