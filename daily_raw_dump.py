import shutil
import schedule
import time
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
from utils import get_long_term_database_connector, get_short_term_database_connector

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


class SupermarketDataPublisherInterface(BaseSupermarketDataPublisher):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def run(self, operations):
        logging.info(f"Starting executing DAG = {operations}")
        self._check_tz()
        for operation in operations.split(","):

            logging.info(f"Starting the operation={operation}")
            if operation == "scraping":
                self._execute_scraping()
            elif operation == "converting":
                self._execute_converting()
            elif operation == "clean_dump_files":
                self._clean_all_dump_files()
            elif operation == "publishing":
                self._upload_and_clean()
            elif operation == "api_update":
                self._update_api_database()    
            elif operation == "reload_complete_api":
                self._update_api_database(reset_cache=True)
            elif operation == "upload_compose":
                self._upload_and_clean(compose=True)
            elif operation == "upload_no_compose":
                self._upload_and_clean(compose=False)
            elif operation == "clean_all_source_data":
                self._clean_all_source_data()
            elif operation == "all":
                self._execute_scraping()
                self._execute_converting()
                self._upload_and_clean()
            else:
                raise ValueError(f"Invalid operation {operation}")


class SupermarketDataPublisher(SupermarketDataPublisherInterface):

    def __init__(
        self,
        long_term_db_target=KaggleUploader,
        short_term_db_target=MongoDbUploader,
        number_of_scraping_processes=4,
        number_of_parseing_processs=None,
        app_folder="app_data",
        data_folder="dumps",
        outputs_folder="outputs",
        status_folder="status",
        enabled_scrapers=None,
        enabled_file_types=None,
        start_at=None,
        completed_by=None,
        num_of_occasions=3,
        limit=None,
        when_date=None,
    ):
        super().__init__(
            number_of_scraping_processes=number_of_scraping_processes,
            number_of_parseing_processs=number_of_parseing_processs,
            long_term_db_target=long_term_db_target,
            short_term_db_target=short_term_db_target,
            app_folder=app_folder,
            data_folder=data_folder,
            outputs_folder=outputs_folder,
            status_folder=status_folder,
            enabled_scrapers=enabled_scrapers,
            enabled_file_types=enabled_file_types,
            limit=limit,
            when_date=when_date,
        )
        self.num_of_occasions = num_of_occasions
        self.completed_by = completed_by if completed_by else self._end_of_day()
        self.start_at = start_at if start_at else self._non()
        self.executed_jobs = 0
        self.occasions = self._compute_occasions()

    def _setup_schedule(self, operations):
        logging.info(f"Scheduling the scraping tasks at {self.occasions}")
        for occasion in self.occasions:
            schedule.every().day.at(occasion).do(self._execute_operations, operations)

    def _execute_operations(self, operations):
        try:
            super().run(operations)
        finally:
            self.executed_jobs += 1
            logging.info("Scraping task is done")

    def _track_task(self):
        logging.info("Starting the tracking tasks")
        while self.executed_jobs < len(self.occasions):
            schedule.run_pending()
            time.sleep(1)
        logging.info("Scraping tasks are done, starting the converting task")

    def _compute_occasions(self):
        """Compute the occasions for the scraping tasks"""
        interval_start = max(self.start_at, self.today)
        interval = (
            self.completed_by - interval_start
        ).total_seconds() / self.num_of_occasions
        occasions = [
            (interval_start + datetime.timedelta(minutes=1)).strftime("%H:%M")
        ] + [
            (interval_start + datetime.timedelta(seconds=interval * (i + 1))).strftime(
                "%H:%M"
            )
            for i in range(1, self.num_of_occasions)
        ]
        return occasions

    def _get_time_to_execute(self):
        return datetime.timedelta(hours=1)

    def _end_of_day(self):
        """Return the end of the day"""
        return (
            datetime.datetime.combine(self.today, datetime.time(23, 59))
            - self._get_time_to_execute()
        )

    def _non(self):
        """Return the start of the day"""
        return datetime.datetime.combine(self.today, datetime.time(12, 0))

    def run(self, itreative_operations, final_operations, now=False):
        if now:
            self._execute_operations(itreative_operations)
            self.executed_jobs = 0

        self._check_tz()
        self._setup_schedule(itreative_operations)
        self._track_task()
        super().run(operations=final_operations)


if __name__ == "__main__":

    limit = os.environ.get("LIMIT", None)
    if limit:
        limit = int(limit)

    publisher = SupermarketDataPublisherInterface(
        app_folder="app_data",
        long_term_db_target=get_long_term_database_connector(),
        short_term_db_target=get_short_term_database_connector(),
        number_of_scraping_processes=min(os.cpu_count(), 3),
        number_of_parseing_processs=min(os.cpu_count(), 3),
        limit=limit,
    )
    publisher.run(operations=os.environ["OPREATION"])
