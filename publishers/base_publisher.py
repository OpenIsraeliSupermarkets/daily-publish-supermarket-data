"""
Base module for supermarket data publishing.
Provides the core functionality for scraping, converting, and uploading supermarket data.
"""
import logging
import datetime
import os
import shutil
from il_supermarket_scarper import ScarpingTask
from il_supermarket_parsers import ConvertingTask
from managers.long_term_database_manager import LongTermDatasetManager
from managers.short_term_database_manager import ShortTermDBDatasetManager
from managers.cache_manager import CacheManager
from remotes import KaggleUploader, MongoDbUploader
from utils import now

logging.getLogger("Logger").setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class BaseSupermarketDataPublisher:
    """
    Base class for publishing supermarket data to various destinations.
    Handles scraping, converting, and uploading data to short-term and long-term databases.
    """

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
        """
        Initialize the BaseSupermarketDataPublisher.

        Args:
            long_term_db_target: Target for long-term database storage
            short_term_db_target: Target for short-term database storage
            number_of_scraping_processes: Number of concurrent scraping processes
            number_of_parseing_processs: Number of parsing processes
            app_folder: Base folder for application data
            data_folder: Subfolder for storing scraped data
            outputs_folder: Subfolder for storing output data
            status_folder: Subfolder for storing status information
            enabled_scrapers: List of enabled scrapers
            enabled_file_types: List of enabled file types
            limit: Limit on the number of items to scrape
            when_date: Date for which to scrape data
        """
        self.short_term_db_target = short_term_db_target
        self.long_term_db_target = long_term_db_target
        self.today = now()
        self.when_date = when_date if when_date else self.today
        self.number_of_scraping_processes = number_of_scraping_processes
        self.number_of_parseing_processs = (
            number_of_parseing_processs
            if number_of_parseing_processs
            else number_of_scraping_processes - 2
        )
        self.app_folder = app_folder
        self.data_folder = os.path.join(app_folder, data_folder)
        self.outputs_folder = os.path.join(app_folder, outputs_folder)
        self.status_folder = os.path.join(app_folder, data_folder, status_folder)
        self.enabled_scrapers = enabled_scrapers
        self.enabled_file_types = enabled_file_types
        self.limit = limit

        logging.info("app_folder=%s", app_folder)

    def _check_tz(self):
        """
        Verify that the system timezone is set to Asia/Jerusalem.

        Raises:
            AssertionError: If the timezone is not correctly set.
        """
        assert (
            datetime.datetime.now().hour == now().hour
        ), "The timezone should be set to Asia/Jerusalem"

    def _execute_scraping(self):
        """
        Execute the scraping task to collect supermarket data.

        Raises:
            Exception: If an error occurs during scraping.
        """
        try:
            os.makedirs(self.data_folder, exist_ok=True)
            
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
            logging.error("An error occurred during scraping: %s", e)
            raise e

    def _execute_converting(self):
        """
        Execute the converting task to parse scraped data into structured format.
        """
        logging.info("Starting the converting task")
        os.makedirs(self.outputs_folder, exist_ok=True)
        
        ConvertingTask(
            enabled_parsers=self.enabled_scrapers,
            files_types=self.enabled_file_types,
            data_folder=self.data_folder,
            multiprocessing=self.number_of_parseing_processs,
            output_folder=self.outputs_folder,
            when_date=datetime.datetime.now(),
        ).start()

        logging.info("Converting task is done")

    def _update_api_database(self, reset_cache=False):
        """
        Update the short-term database with the converted data.

        Args:
            reset_cache: Whether to force a restart of the cache (default: False).
        """
        logging.info("Starting the short term database task")
        database = ShortTermDBDatasetManager(
            short_term_db_target=self.short_term_db_target,
            app_folder=self.app_folder,
            outputs_folder=self.outputs_folder,
            status_folder=self.status_folder,
        )
        database.upload(force_restart=reset_cache)

    def _upload_to_kaggle(self):
        """
        Upload the data to the long-term database (Kaggle by default).
        """
        logging.info("Starting the long term database task")
        database = LongTermDatasetManager(
            long_term_db_target=self.long_term_db_target,
            enabled_scrapers=self.enabled_scrapers,
            enabled_file_types=self.enabled_file_types,
            outputs_folder=self.outputs_folder,
            status_folder=self.status_folder,
        )
        database.compose()
        database.upload()
        # clean the dataset only if the data was uploaded successfully
        # (upload_to_dataset raise an exception)
        # if not, "compose" will clean it next time
        database.clean()

    def _upload_and_clean(self, compose=True):
        """
        Upload data to Kaggle and clean up afterward, regardless of success.

        Args:
            compose: Whether to compose the dataset before uploading (default: True).
                     This parameter is maintained for compatibility but is not used
                     in the current implementation.

        Raises:
            ValueError: If uploading to Kaggle fails.
        """
        try:
            # compose parameter is maintained for API compatibility
            # but is not used in current implementation
            self._upload_to_kaggle()
        except ValueError as e:
            logging.error("Failed to upload to kaggle")
            raise e
        finally:
            # clean data allways after uploading
            self._clean_all_source_data()

    def _clean_all_dump_files(self):
        """
        Clean all dump files in the data folder, preserving the status folder.
        """
        # Clean the folders in case of an error
        for folder in [self.data_folder]:
            if os.path.exists(folder):
                for filename in os.listdir(folder):
                    file_path = os.path.join(folder, filename)
                    if file_path != self.status_folder:
                        shutil.rmtree(file_path)

    def _clean_all_source_data(self):
        """
        Clean all source data, including the data, outputs, and status folders.
        """
        # Clean the folders in case of an error
        for folder in [self.data_folder, self.outputs_folder, self.status_folder]:
            if os.path.exists(folder):
                shutil.rmtree(folder)

        with CacheManager(self.app_folder) as cache:
            cache.clear()
