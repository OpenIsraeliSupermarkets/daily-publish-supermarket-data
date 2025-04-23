from remotes import KaggleUploader, MongoDbUploader
from publishers.dag_publisher import SupermarketDataPublisherInterface
import os
from utils import now
import logging

if __name__ == "__main__":

    logging.getLogger("Logger").setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    when = now()
    
    limit = os.environ.get("LIMIT", None)
    if limit:
        limit = int(limit)
        
    enabled_scrapers = os.environ.get("ENABLED_SCRAPERS", "")
    if enabled_scrapers == "":
        enabled_scrapers = None
    else:
        enabled_scrapers = enabled_scrapers.split(",")


    enabled_file_types = os.environ.get("ENABLED_FILE_TYPES", "")
    if enabled_file_types == "":
        enabled_file_types = None
    else:
        enabled_file_types = enabled_file_types.split(",")
    
    
    logging.info(f"Enabled scrapers: {enabled_scrapers}")
    logging.info(f"Enabled file types: {enabled_file_types}")
    logging.info(f"Limit: {limit}")
    logging.info(f"When: {when}")
    
    publisher = SupermarketDataPublisherInterface(
        number_of_scraping_processes=min(os.cpu_count(), 3),
        number_of_parseing_processs=min(os.cpu_count(), 3),
        app_folder=os.environ["APP_DATA_PATH"],
        data_folder="dumps",
        outputs_folder="outputs",
        status_folder="status",
        enabled_scrapers=enabled_scrapers,
        enabled_file_types=enabled_file_types,
        long_term_db_target=KaggleUploader(
            dataset_path=os.environ["KAGGLE_DATASET_REMOTE_NAME"], # make the folder the same name
            dataset_remote_name=os.environ["KAGGLE_DATASET_REMOTE_NAME"],
            when=when,
        ),
        short_term_db_target=MongoDbUploader(
            mongodb_uri=os.environ["MONGODB_URI"]
        ),
        limit=limit,
        when_date=when,
    )
    publisher.run(operations=os.environ["OPREATION"])
