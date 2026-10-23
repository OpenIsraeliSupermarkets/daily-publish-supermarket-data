"""
Base module for supermarket data publishing.
Provides the core functionality for scraping, converting, and uploading supermarket data.
"""

from utils import Logger
import datetime
import os
import shutil
from il_supermarket_scarper import ScarpingTask, ScraperFactory
from il_supermarket_parsers import ConvertingTask, FileTypesFilters
from managers.long_term_database_manager import LongTermDatasetManager
from managers.short_term_database_manager import ShortTermDBDatasetManager
from managers.cache_manager import CacheManager
from managers.stream_push_manager import StreamPushManager
from managers.file_type_router import FileTypeRouter
from managers.quality_indicators import write_quality_indicators
from remotes import KaggleUploader, MongoDbUploader
from utils import now, use_forkserver


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
        status_configuration=None,
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
        self.when_date = when_date
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
        self.enabled_scrapers = (
            enabled_scrapers if enabled_scrapers else ScraperFactory.all_scrapers_name()
        )
        self.enabled_file_types = (
            enabled_file_types if enabled_file_types else FileTypesFilters.all_types()
        )
        self.limit = limit
        self.status_configuration = status_configuration or {"database_type": "json"}
        self.scraping_status_folder = os.path.join(app_folder, "scraping_status")
        self.converting_status_folder = os.path.join(app_folder, "converting_status")
        self.quality_folder = os.path.join(app_folder, "quality")

        Logger.info("app_folder=%s", app_folder)

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

            Logger.info("Starting the scraping task")
            task = ScarpingTask(
                enabled_scrapers=self.enabled_scrapers,
                files_types=self.enabled_file_types,
                multiprocessing=self.number_of_scraping_processes,
                status_configuration={
                    **self.status_configuration,
                    "base_path": self.scraping_status_folder,
                },
                output_configuration={
                    "output_mode": "disk",
                    "base_storage_path": self.data_folder,
                },
            )
            task.start(
                limit=self.limit,
                when_date=self.when_date if self.when_date else now(backfill_hours=1),
            )
            task.join()
            Logger.info("Scraping task is done")
        except Exception as e:
            Logger.error("An error occurred during scraping: %s", e)
            raise e

    def _execute_converting(self):
        """
        Execute the converting task to parse scraped data into structured format.
        """
        output_configuration = [
            {
                "output_mode": "csv",
                "output_folder": self.outputs_folder,
            }
        ]
        task = ConvertingTask(
            enabled_parsers=self.enabled_scrapers,
            files_types=self.enabled_file_types,
            multiprocessing=self.number_of_parseing_processs,
            status_configuration={
                **self.status_configuration,
                "base_path": self.converting_status_folder,
            },
            output_configuration=output_configuration,
            source_configuration={
                "folder": self.data_folder,
            },
        )
        task.start()
        task.join()
        Logger.info("Converting task is done")
        write_quality_indicators(
            quality_folder=self.quality_folder,
            scraping_status_folder=self.scraping_status_folder,
            converting_status_folder=self.converting_status_folder,
            enabled_scrapers=self.enabled_scrapers,
            enabled_file_types=self.enabled_file_types,
        )

    def _execute_streaming(self):
        """
        Scrape, convert and push in one pipeline, the way the parser's
        example.py does: the scraper hands files to the converter through
        queues, and the converter writes every row both to the CSV outputs
        (for the long-term upload and api_update) and to a queue that is
        pushed to the short-term database as rows arrive (#114).
        """
        os.makedirs(self.data_folder, exist_ok=True)
        # The scraper and converters start their pools from background threads
        # while the router and push threads run; main.py already does this at
        # startup, repeated here for callers that skip main.py (#116).
        use_forkserver()
        scraper = ScarpingTask(
            enabled_scrapers=self.enabled_scrapers,
            files_types=self.enabled_file_types,
            multiprocessing=self.number_of_scraping_processes,
            status_configuration={
                **self.status_configuration,
                "base_path": self.scraping_status_folder,
            },
            output_configuration={"output_mode": "queue", "queue_type": "memory"},
        )
        router = FileTypeRouter(
            scraper.consume(), self.enabled_file_types, self.scraping_status_folder
        )
        # One converter per file type, each reading only its own files. Each
        # has number_of_parseing_processs workers, so the chains being scraped
        # at the same time are parsed at the same time.
        converters = [
            ConvertingTask(
                enabled_parsers=self.enabled_scrapers,
                files_types=[file_type],
                multiprocessing=self.number_of_parseing_processs,
                status_configuration={
                    **self.status_configuration,
                    "base_path": self.converting_status_folder,
                },
                source_configuration={"queue_handlers": router.queues[file_type]},
                output_configuration=[
                    {"output_mode": "queue", "queue_type": "memory"},
                    {"output_mode": "csv", "output_folder": self.outputs_folder},
                ],
            )
            for file_type in router.file_types
        ]
        # queues must exist before the converters' workers start
        parsed_rows = {}
        for converter in converters:
            parsed_rows.update(converter.consume())

        with CacheManager(self.app_folder) as local_cache:
            if local_cache.is_empty():
                self.short_term_db_target.restart_database(
                    self.enabled_scrapers, self.enabled_file_types
                )
            Logger.info("Starting the streaming task")
            scraper.start(
                limit=self.limit,
                when_date=self.when_date if self.when_date else now(backfill_hours=1),
            )
            router.start()
            for converter in converters:
                converter.start()
            try:
                StreamPushManager(self.short_term_db_target).push(
                    parsed_rows, local_cache
                )
            finally:
                scraper.join()
                router.join()
                for converter in converters:
                    try:
                        converter.join()
                    except RuntimeError:
                        pass  # already finished: join() raises once the thread is done
        Logger.info("Streaming task is done")
        write_quality_indicators(
            quality_folder=self.quality_folder,
            scraping_status_folder=self.scraping_status_folder,
            converting_status_folder=self.converting_status_folder,
            enabled_scrapers=self.enabled_scrapers,
            enabled_file_types=self.enabled_file_types,
        )

    def _download_from_long_term_database(self):
        """
        Download the data from the long-term database.
        """
        Logger.info("Starting the long term database task")
        database = LongTermDatasetManager(
            long_term_db_target=self.long_term_db_target,
            enabled_scrapers=self.enabled_scrapers,
            enabled_file_types=self.enabled_file_types,
            outputs_folder=self.outputs_folder,
            status_folder=self.status_folder,
        )
        return database.download()

    def _update_api_database(self, reset_cache=False):
        """
        Update the short-term database with the converted data.

        Args:
            reset_cache: Whether to force a restart of the cache (default: False).
        """
        Logger.info("Starting the short term database task")
        database = ShortTermDBDatasetManager(
            short_term_db_target=self.short_term_db_target,
            app_folder=self.app_folder,
            outputs_folder=self.outputs_folder,
            status_folder=self.status_folder,
            enabled_scrapers=self.enabled_scrapers,
            enabled_file_types=self.enabled_file_types,
            scraping_status_folder=self.scraping_status_folder,
            converting_status_folder=self.converting_status_folder,
        )
        database.upload(force_restart=reset_cache)

    def _upload_to_kaggle(self):
        """
        Upload the data to the long-term database (Kaggle by default).
        """
        Logger.info("Starting the long term database task")
        database = LongTermDatasetManager(
            long_term_db_target=self.long_term_db_target,
            enabled_scrapers=self.enabled_scrapers,
            enabled_file_types=self.enabled_file_types,
            outputs_folder=self.outputs_folder,
            scraping_status_folder=self.scraping_status_folder,
            converting_status_folder=self.converting_status_folder,
            quality_folder=self.quality_folder,
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
        except Exception as e:
            Logger.error("Failed to upload to kaggle")
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
        for folder in [
            self.data_folder,
            self.outputs_folder,
            self.converting_status_folder,
            self.scraping_status_folder,
            self.quality_folder,
        ]:
            if os.path.exists(folder):
                shutil.rmtree(folder)

        with CacheManager(self.app_folder) as cache:
            cache.clear()
