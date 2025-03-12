import os
import shutil
import json
import datetime
import pytz
import logging
from remotes import KaggleUploader


class LongTermDatasetManager:
    def __init__(
        self,
        long_term_db_target=KaggleUploader,
        app_folder=".",
        enabled_scrapers=None,
        enabled_file_types=None,
        dataset="israeli-supermarkets-2024",
    ):

        self.when = self._now()
        self.dataset = dataset
        self.enabled_scrapers = (
            "ALL" if not enabled_scrapers else ",".join(enabled_scrapers)
        )
        self.enabled_file_types = (
            "ALL" if not enabled_file_types else ",".join(enabled_file_types)
        )
        self.dataset_path = os.path.join(app_folder, self.dataset)
        self.remote_database = long_term_db_target(
            dataset_path=self.dataset_path, when=self.when
        )
        logging.info(f"Dataset path: {self.dataset_path}")

    def _now(self):
        return datetime.datetime.now(pytz.timezone("Asia/Jerusalem")).strftime(
            "%d/%m/%Y, %H:%M:%S"
        )

    def read_parser_status(self, outputs_folder):
        with open(f"{outputs_folder}/parser-status.json", "r") as file:
            data = json.load(file)

        descriptions = []
        for entry in data:

            if "response" in entry and entry["response"]["file_was_created"]:
                descriptions.append(
                    {
                        "path": os.path.split(entry["response"]["file_created_path"])[
                            -1
                        ],
                        "description": f"{len(entry['response']['files_to_process'])} XML files from type {entry['response']['files_types']} published by '{entry['store_enum']}' ",
                    }
                )

        return descriptions

    def read_scraper_status_files(self, status_folder):
        descriptions = []
        for file in os.listdir(status_folder):
            if file.endswith(".json"):
                descriptions.append(
                    {
                        "path": file,
                        "description": f"Scraper status file for '{file}' execution.",
                    }
                )
        return descriptions

    def compose(self, outputs_folder, status_folder):
        if not os.path.exists(self.dataset_path):
            os.makedirs(self.dataset_path, exist_ok=True)
            with open(f"{self.dataset_path}/dataset-metadata.json", "w") as file:
                json.dump(
                    {
                        "title": "Israeli Supermarkets 2024",
                        "id": f"erlichsefi/{self.dataset}",
                        "resources": [
                            {
                                "path": "index.json",
                                "description": "Index mapping between Kaggle versions and dataset creation times",
                            },
                            {
                                "path": "parser-status.json",
                                "description": "Parser status file",
                            },
                        ]
                        + self.read_parser_status(outputs_folder)
                        + self.read_scraper_status_files(status_folder),
                    },
                    file,
                )
            shutil.copytree(outputs_folder, self.dataset_path, dirs_exist_ok=True)
            shutil.copytree(status_folder, self.dataset_path, dirs_exist_ok=True)

            self.remote_database.increase_index()

    def upload(self):
        """
        Upload a new file to an existing Kaggle dataset.

        :param dataset: str, the dataset to upload to in the format 'owner/dataset-name'
        :param file_path: str, the path to the file to upload
        :param new_file_name: str, optional new name for the file in the dataset
        """
        try:
            self.remote_database.upload_to_dataset(
                message=f"Update-Time: {self.when}, Scrapers:{self.enabled_scrapers}, Files:{self.enabled_file_types}"
            )
        except Exception as e:
            logging.critical(f"Error uploading file: {e}")
            raise ValueError(f"Error uploading file: {e}")

    def clean(self):
        shutil.rmtree(self.dataset_path)
        self.remote_database.clean()
