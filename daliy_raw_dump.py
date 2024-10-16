import shutil
import schedule
import time
import logging
import datetime
import os
from il_supermarket_scarper import ScarpingTask, ScraperFactory
from il_supermarket_parsers import ConvertingTask
from kaggle_database_manager import KaggleDatasetManager


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class SupermarketDataPublisher:

    def __init__(
        self,
        number_of_processes=4,
        data_folder="app_data/dumps",
        outputs_folder="app_data/outputs",
        status_folder="app_data/dumps/status",
        enabled_scrapers=None,
        enabled_file_types=None,
        occasions=None,
    ):
        self.number_of_processes = number_of_processes
        self.data_folder = data_folder
        self.outputs_folder = outputs_folder
        self.status_folder = status_folder
        self.enabled_scrapers = (
            enabled_scrapers
            if enabled_scrapers is not None
            else [ScraperFactory.BAREKET.name]
        )
        self.enabled_file_types = enabled_file_types
        self.occasions = (
            occasions if occasions is not None else ["12:00", "17:30", "23:00"]
        )

        assert (
            os.environ["TZ"] == "Asia/Jerusalem"
        ), "The timezone should be set to Asia/Jerusalem"
        self.today = datetime.datetime.now()
        self.executed_jobs = 0

    def run_scraping(self):
        try:
            logging.info("Starting the scraping task")
            ScarpingTask(
                enabled_scrapers=self.enabled_scrapers,
                files_types=self.enabled_file_types,
                dump_folder_name=self.data_folder,
                multiprocessing=self.number_of_processes,
                lookup_in_db=True,
                when_date=self.today,
            ).start()
        except Exception as e:
            logging.error(f"An error occurred during scraping: {e}")
        finally:
            self.executed_jobs += 1
            logging.info("Scraping task is done")

    def _setup_schedule(self):
        logging.info(f"Scheduling the scraping tasks at {self.occasions}")
        for occasion in self.occasions:
            schedule.every().day.at(occasion).do(self.run_scraping)

    def _execute_scraping(self):
        logging.info("Starting the scraping tasks")
        while self.executed_jobs < len(self.occasions):
            schedule.run_pending()
            time.sleep(1)
        logging.info("Scraping tasks are done, starting the converting task")

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
            dataset="israeli-supermarkets-2024",
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

    def run(self):
        try:

            self._setup_schedule()
            self._execute_scraping()
            self._execute_converting()
            self._upload_to_kaggle()
        finally:
            self._clean_folders()


if __name__ == "__main__":
    publisher = SupermarketDataPublisher()
    publisher.run()
