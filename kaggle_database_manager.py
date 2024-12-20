import os
import shutil
import json
import datetime
import pytz
from kaggle import KaggleApi
import logging


class KaggleDatasetManager:
    def __init__(
        self, dataset, app_folder=".", enabled_scrapers=None, enabled_file_types=None
    ):
        self.api = KaggleApi()
        self.api.authenticate()
        self.dataset = dataset
        self.when = self._now()
        self.enabled_scrapers = (
            "ALL" if not enabled_scrapers else ",".join(enabled_scrapers)
        )
        self.enabled_file_types = (
            "ALL" if not enabled_file_types else ",".join(enabled_file_types)
        )
        self.dataset_path = os.path.join(app_folder, self.dataset)
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
        shutil.rmtree(self.dataset_path, ignore_errors=True)
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

        self.increase_index()

    def increase_index(self):
        """
        Download a dataset from Kaggle.

        :param dataset: str, the dataset to download in the format 'owner/dataset-name'
        :param path: str, the path where to save the dataset (default is current directory)
        """

        self.api.dataset_download_cli(
            f"erlichsefi/{self.dataset}", file_name="index.json", force=True
        )
        print(f"Dataset '{self.dataset}' downloaded successfully")

        with open("index.json", "r") as file:
            index = json.load(file)

        index[max(map(int, index.keys())) + 1] = self.when

        with open(os.path.join(self.dataset_path, "index.json"), "w+") as file:
            json.dump(index, file)

    def upload_to_dataset(self):
        """
        Upload a new file to an existing Kaggle dataset.

        :param dataset: str, the dataset to upload to in the format 'owner/dataset-name'
        :param file_path: str, the path to the file to upload
        :param new_file_name: str, optional new name for the file in the dataset
        """
        try:
            self.api.dataset_create_version(
                self.dataset_path,
                version_notes=f"Update-Time: {self.when}, Scrapers:{self.enabled_scrapers}, Files:{self.enabled_file_types}",
                delete_old_versions=False,
            )
        except Exception as e:
            logging.critical(f"Error uploading file: {e}")
            raise ValueError(f"Error uploading file: {e}")

    def clean(self):
        shutil.rmtree(self.dataset_path)
        os.remove("index.json")


# Example usage:
if __name__ == "__main__":
    manager = KaggleDatasetManager("israeli-supermarkets-2024")

    # Upload a folder to a dataset (make sure you have write permissions)
    manager.increase_index()
