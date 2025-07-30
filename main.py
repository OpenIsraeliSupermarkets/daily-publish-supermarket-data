from remotes import KaggleUploader, MongoDbUploader
from publishers.dag_publisher import SupermarketDataPublisherInterface
import os
import datetime
from utils import now
import logging
import datetime
import pytz

if __name__ == "__main__":

    logging.getLogger("Logger").setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Allow providing when from environment variable, otherwise use current time
    when_str = os.environ.get("WHEN", None)
    if when_str:
        # Parse the datetime string from environment variable
        when = datetime.datetime.fromisoformat(when_str.replace('Z', '+00:00'))
        if when.tzinfo is None:
            # If no timezone info, assume Jerusalem timezone
            import pytz
            when = pytz.timezone("Asia/Jerusalem").localize(when)
    else:
        when = now()
    
    
    num_of_processes = os.environ.get("NUM_OF_PROCESSES", 5)
    try:
        num_of_processes = int(num_of_processes)
    except ValueError:
        num_of_processes = os.cpu_count()
    
    
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
    
    logging.info(f"Number of processes: {num_of_processes}")
    logging.info(f"Enabled scrapers: {enabled_scrapers}")
    logging.info(f"Enabled file types: {enabled_file_types}")
    logging.info(f"Limit: {limit}")
    logging.info(f"When: {when}")
    
    

    publisher = SupermarketDataPublisherInterface(
        number_of_scraping_processes=num_of_processes,
        number_of_parseing_processs=num_of_processes,
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
    publisher.run(operations=os.environ["OPERATION"])
