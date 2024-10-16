import shutil
import schedule
import time
import logging
import datetime
import pytz

import os
from il_supermarket_scarper import ScarpingTask,ScraperFactory
from il_supermarket_parsers import ConvertingTask
from kaggle_database_manager import KaggleDatasetManager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


number_of_processes = 4
data_folder = "app_data/dumps"
outputs_folder = "app_data/outputs"
status_folder = "app_data/dumps/status"
enabled_scrapers = [ScraperFactory.BAREKET.name]
enabled_file_types = None
occasions = ["12:04", "12:05"]
today = datetime.datetime.now(pytz.timezone("Asia/Jerusalem"))
executed_jobs = 0


def run_scraping():
    global executed_jobs
    try:
        logging.info("Starting the scraping task")
        ScarpingTask(
            enabled_scrapers=enabled_scrapers,
            files_types=enabled_file_types,
            dump_folder_name=data_folder,
            multiprocessing=number_of_processes,
            lookup_in_db=True,
            when_date=today,
            limit=1
        ).start()
    except Exception:
        pass
    finally:
        executed_jobs+=1
        logging.info("Scraping task is done")


if __name__ == "__main__":

    try:
        #
        logging.info(f"Sceduling the scraping tasks in {occasions}")
        for occasion in occasions:
            schedule.every().day.at(occasion).do(run_scraping)

        #
        logging.info("Starting the scraping tasks")
        while executed_jobs < len(occasions):
            schedule.run_pending()
            time.sleep(1)
        logging.info(f"Scraping tasks are done, starting the converting task")

        #
        logging.info("Starting the converting task")
        ConvertingTask(
            enabled_parsers=enabled_scrapers,
            files_types=enabled_file_types,
            data_folder=data_folder,
            multiprocessing=number_of_processes,
            output_folder=outputs_folder,
        ).start()

        # logging.info("Converting task is done, starting the database task")
        # database = KaggleDatasetManager(
        #     dataset="israeli-supermarkets-2024",
        #     enabled_scrapers=enabled_scrapers,
        #     enabled_file_types=enabled_file_types,
        # )
        # database.compose(outputs_folder=outputs_folder, status_folder=status_folder)
        # database.upload_to_dataset()
        # database.clean()

    finally:
        # clean the folders in case of an error
        for folder in [data_folder, outputs_folder, status_folder]:

            if os.path.exists(folder):
                shutil.rmtree(folder)
