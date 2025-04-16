
import shutil
import pytz
import logging
import datetime
import os
from il_supermarket_scarper import ScarpingTask
from il_supermarket_parsers import ConvertingTask
from managers.long_term_database_manager import LongTermDatasetManager
from managers.short_term_database_manager import ShortTermDBDatasetManager
from remotes import (
    KaggleUploader,
    MongoDbUploader
)

logging.getLogger("Logger").setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

class BaseSupermarketDataPublisher:

    def __init__(
        self,
        long_term_db_target=KaggleUploader,
        short_term_db_target=MongoDbUploader,
        number_of_scraping_processes=3,
        number_of_parseing_processs=None,
        app_folder="app_data",
        data_folder="dumps",
        outputs_folder="outputs",
        status_folder="status",
        enabled_scrapers=None,
        enabled_file_types=None,
        limit=None,
        when_date=None,
    ):
        self.short_term_db_target = short_term_db_target
        self.long_term_db_target = long_term_db_target
        self.today = datetime.datetime.now()
        self.when_date = when_date if when_date else self.today
        self.number_of_scraping_processes = number_of_scraping_processes
        self.number_of_parseing_processs = (
            number_of_parseing_processs
            if number_of_parseing_processs
            else number_of_scraping_processes - 2
        )
        self.app_folder = app_folder
        self.data_folder = os.path.join(app_folder, self._dump_folder_name(data_folder))
        self.outputs_folder = os.path.join(app_folder, outputs_folder)
        self.status_folder = os.path.join(
            app_folder, self._dump_folder_name(data_folder), status_folder
        )
        self.enabled_scrapers = enabled_scrapers
        self.enabled_file_types = enabled_file_types
        self.limit = limit

        logging.info(f"app_folder={app_folder}")

    def _dump_folder_name(self, data_folder):
        return data_folder  # f"{data_folder}_{self.today.strftime('%Y%m%d')}" # TBD: if we want to add the date we need to make sure hte publisher will get the correct date

    def _check_tz(self):
        assert (
            datetime.datetime.now().hour
            == datetime.datetime.now(pytz.timezone("Asia/Jerusalem")).hour
        ), "The timezone should be set to Asia/Jerusalem"

    def _execute_scraping(self):
        try:
            logging.info("Starting the scraping task")
            ScarpingTask(
                enabled_scrapers=self.enabled_scrapers,
                files_types=self.enabled_file_types,
                dump_folder_name=self.data_folder,
                multiprocessing=self.number_of_scraping_processes,
                lookup_in_db=True,
                when_date=self.when_date,
                limit=self.limit,
                suppress_exception=True,
            ).start()
            logging.info("Scraping task is done")
        except Exception as e:
            logging.error(f"An error occurred during scraping: {e}")
            raise e

    def _execute_converting(self):
        logging.info("Starting the converting task")
        ConvertingTask(
            enabled_parsers=self.enabled_scrapers,
            files_types=self.enabled_file_types,
            data_folder=self.data_folder,
            multiprocessing=self.number_of_parseing_processs,
            output_folder=self.outputs_folder,
        ).start()

        logging.info("Converting task is done")

    def _update_api_database(self,reset_cache=False):
        logging.info("Starting the short term database task")
        database = ShortTermDBDatasetManager(
            short_term_db_target=self.short_term_db_target,
            app_folder=self.app_folder,
            outputs_folder=self.outputs_folder, 
            status_folder=self.status_folder,
        )
        database.upload(
            force_restart=reset_cache
        )

    def _upload_to_kaggle(self, compose=True):
        logging.info("Starting the long term database task")
        database = LongTermDatasetManager(
            long_term_db_target=self.long_term_db_target,
            enabled_scrapers=self.enabled_scrapers,
            enabled_file_types=self.enabled_file_types,
            app_folder=self.app_folder,
            outputs_folder=self.outputs_folder, 
            status_folder=self.status_folder
        )
        database.compose()
        database.upload()
        # clean the dataset only if the data was uploaded successfully (upload_to_dataset raise an exception)
        # if not, "compose" will clean it next time
        database.clean()

    def _upload_and_clean(self, compose=True):
        try:
            self._upload_to_kaggle(compose=compose)
        except ValueError as e:
            logging.error("Failed to upload to kaggle")
            raise e
        finally:
            # clean data allways after uploading
            self._clean_all_source_data()

    def _clean_all_dump_files(self):
        # Clean the folders in case of an error
        for folder in [self.data_folder]:
            if os.path.exists(folder):
                for filename in os.listdir(folder):
                    file_path = os.path.join(folder, filename)
                    if file_path != self.status_folder:
                        shutil.rmtree(file_path)

    def _clean_all_source_data(self):
        # Clean the folders in case of an error
        for folder in [self.data_folder, self.outputs_folder, self.status_folder]:
            if os.path.exists(folder):
                shutil.rmtree(folder)

        for folder in [self.app_folder]:
            for root, dirs, files in os.walk(folder):
                for file in files:
                    os.remove(os.path.join(root, file))
                for dir in dirs:
                    shutil.rmtree(os.path.join(root, dir))