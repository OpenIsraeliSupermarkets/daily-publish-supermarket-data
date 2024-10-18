import shutil
import schedule
import time
import pytz
import logging
import datetime
import os
from il_supermarket_scarper import ScarpingTask
from il_supermarket_parsers import ConvertingTask
from kaggle_database_manager import KaggleDatasetManager
import sys


logging.getLogger("Logger").setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class BaseSupermarketDataPublisher:

    def __init__(
        self,
        number_of_processes=4,
        app_folder="app_data",
        data_folder="dumps",
        outputs_folder="outputs",
        status_folder="status",
        enabled_scrapers=None,
        enabled_file_types=None,
        limit=None,
    ):
        self.today = datetime.datetime.now()
        self.number_of_processes = number_of_processes
        self.data_folder = os.path.join(app_folder, self._dump_folder_name(data_folder))
        self.outputs_folder = os.path.join(app_folder, outputs_folder)
        self.status_folder = os.path.join(
            app_folder, self._dump_folder_name(data_folder), status_folder
        )
        self.enabled_scrapers = enabled_scrapers
        self.enabled_file_types = enabled_file_types
        self.limit = limit

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
                multiprocessing=self.number_of_processes,
                lookup_in_db=True,
                when_date=self.today,
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
            multiprocessing=self.number_of_processes,
            output_folder=self.outputs_folder,
        ).start()

        logging.info("Converting task is done")

    def _upload_to_kaggle(self):
        logging.info("Starting the database task")
        database = KaggleDatasetManager(
            dataset="israeli-supermarkets-data",
            enabled_scrapers=self.enabled_scrapers,
            enabled_file_types=self.enabled_file_types,
        )
        database.compose(
            outputs_folder=self.outputs_folder, status_folder=self.status_folder
        )
        database.upload_to_dataset()
        database.clean()

    def _clean_folders(self):
        # Clean the folders in case of an error
        for folder in [self.data_folder, self.outputs_folder, self.status_folder]:
            if os.path.exists(folder):
                shutil.rmtree(folder)


class SupermarketDataPublisher(BaseSupermarketDataPublisher):

    def __init__(
        self,
        number_of_processes=4,
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
    ):
        super().__init__(
            number_of_processes,
            app_folder=app_folder,
            data_folder=data_folder,
            outputs_folder=outputs_folder,
            status_folder=status_folder,
            enabled_scrapers=enabled_scrapers,
            enabled_file_types=enabled_file_types,
            limit=limit,
        )
        self.num_of_occasions = num_of_occasions
        self.completed_by = completed_by if completed_by else self._end_of_day()
        self.start_at = start_at if start_at else self._non()
        self.executed_jobs = 0
        self.occasions = self._compute_occasions()

    def _setup_schedule(self):
        logging.info(f"Scheduling the scraping tasks at {self.occasions}")
        for occasion in self.occasions:
            schedule.every().day.at(occasion).do(self._execute_scraping)

    def _execute_scraping(self):
        try:
            super()._execute_scraping()
        finally:
            self.executed_jobs += 1
            logging.info("Scraping task is done")

    def _track_scraping(self):
        logging.info("Starting the scraping tasks")
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

    def run(self):
        self._check_tz()
        try:
            self._setup_schedule()
            self._track_scraping()
            self._execute_converting()
            self._upload_to_kaggle()
        finally:
            self._clean_folders()


class SupermarketDataPublisherInterface(BaseSupermarketDataPublisher):

    def __init__(self, operation, **kwargs):
        super().__init__(**kwargs)
        self.operation = operation

    def run(self):
        logging.info(f"Starting the operation={self.operation}")
        self._check_tz()
        if self.operation == "scraping":
            self._execute_scraping()
        elif self.operation == "publishing":
            self._execute_converting()
            self._upload_to_kaggle()
            self._clean_folders()
        elif self.operation == "all":
            self._execute_scraping()
            self._execute_converting()
            self._upload_to_kaggle()
            self._clean_folders()
        else:
            raise ValueError(f"Invalid operation {self.operation}")


if __name__ == "__main__":
    publisher = SupermarketDataPublisherInterface(operation=os.environ["OPREATION"])
    publisher.run()
