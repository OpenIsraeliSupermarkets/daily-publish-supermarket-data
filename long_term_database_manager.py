import os
import json
import logging
from remotes import KaggleUploader
from utils import now


class LongTermDatasetManager:
    """
    This class is used to manage the long term database for the supermarket data.
    WHich mean abstracting the remote database uploader and the local folder structure.
    """
    def __init__(
        self,
        app_folder,
        outputs_folder, 
        status_folder,
        dataset_remote_name,
        long_term_db_target=KaggleUploader,
        enabled_scrapers=None,
        enabled_file_types=None
        
    ):
        self.when = now()
        self.enabled_scrapers = (
            "ALL" if not enabled_scrapers else ",".join(enabled_scrapers)
        )
        self.enabled_file_types = (
            "ALL" if not enabled_file_types else ",".join(enabled_file_types)
        )
        self.remote_database_manager = long_term_db_target(
            dataset_remote_name=dataset_remote_name,
            dataset_path=os.path.join(app_folder, "dataset"), 
            when=self.when
        )
        self.outputs_folder = outputs_folder
        self.status_folder = status_folder


    def read_parser_status(self):
        """
        Read the parser status file and return a list of descriptions.
        """
        with open(f"{self.outputs_folder}/parser-status.json", "r") as file:
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

    def read_scraper_status_files(self):
        """
        Read the scraper status files and return a list of descriptions.
        """
        descriptions = []
        for file in os.listdir(self.status_folder):
            if file.endswith(".json"):
                descriptions.append(
                    {
                        "path": file,
                        "description": f"Scraper status file for '{file}' execution.",
                    }
                )
        return descriptions

    def compose(self):
        """
        load the data we would like to upload to the local stagee database.
        """
        self.remote_database_manager.stage(self.outputs_folder)
        self.remote_database_manager.stage(self.status_folder)        
        self.remote_database_manager.increase_index()

    def upload(self):
        """
        Upload a new file to an existing Kaggle dataset.

        :param dataset: str, the dataset to upload to in the format 'owner/dataset-name'
        :param file_path: str, the path to the file to upload
        :param new_file_name: str, optional new name for the file in the dataset
        """
        resources  = {
                "title": "Israeli Supermarkets 2024",
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
                + self.read_parser_status()
                + self.read_scraper_status_files(),
        }
        try:
            self.remote_database_manager.upload_to_dataset(
                message=f"Update-Time: {self.when}, Scrapers:{self.enabled_scrapers}, Files:{self.enabled_file_types}",
                **resources
            )
        except Exception as e:
            logging.critical(f"Error uploading file: {e}")
            raise ValueError(f"Error uploading file: {e}")

    def clean(self):
        self.remote_database_manager.clean()
