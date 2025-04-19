from remotes import KaggleUploader, MongoDbUploader
from publishers.dag_publisher import SupermarketDataPublisherInterface
import os
from datetime import datetime
if __name__ == "__main__":

    limit = os.environ.get("LIMIT", None)
    if limit:
        limit = int(limit)

    publisher = SupermarketDataPublisherInterface(
        number_of_scraping_processes=min(os.cpu_count(), 3),
        number_of_parseing_processs=min(os.cpu_count(), 3),
        app_folder="app_data",
        data_folder="dumps",
        outputs_folder="outputs",
        status_folder="status",
        enabled_scrapers=None,
        enabled_file_types=None,
        long_term_db_target=KaggleUploader(
            dataset_path=os.environ["KAGGLE_DATASET_PATH"],
            dataset_remote_name=os.environ["KAGGLE_DATASET_REMOTE_NAME"],
            when=datetime.now(),
        ),
        short_term_db_target=MongoDbUploader(
            db_path=os.environ["MONGO_DB_PATH"]
        ),
        limit=limit,
    )
    publisher.run(operations=os.environ["OPREATION"])
