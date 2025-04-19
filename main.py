from remotes import KaggleUploader, MongoDbUploader
from publishers.dag_publisher import SupermarketDataPublisherInterface
import os
from utils import now
if __name__ == "__main__":

    when = now()
    
    limit = os.environ.get("LIMIT", None)
    if limit:
        limit = int(limit)

    publisher = SupermarketDataPublisherInterface(
        number_of_scraping_processes=min(os.cpu_count(), 3),
        number_of_parseing_processs=min(os.cpu_count(), 3),
        app_folder=os.environ["APP_DATA_PATH"],
        data_folder="dumps",
        outputs_folder="outputs",
        status_folder="status",
        enabled_scrapers=None,
        enabled_file_types=None,
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
