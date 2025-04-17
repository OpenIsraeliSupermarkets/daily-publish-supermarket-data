import os
import json
import logging
import shutil
from remotes import LongTermDatabaseUploader
from utils import now


class LongTermDatasetManager:
    """
    Manages the long-term storage and organization of supermarket data.
    
    This class provides an abstraction layer for handling both local and remote storage
    of supermarket data, including parser outputs and scraper status files. It manages
    the staging, uploading, and versioning of datasets to remote storage platforms.
    
    Attributes:
        when (datetime): Timestamp of the current operation
        enabled_scrapers (str): Comma-separated list of enabled scrapers or "ALL"
        enabled_file_types (str): Comma-separated list of enabled file types or "ALL"
        remote_database_manager: Instance of the remote database uploader
        outputs_folder (str): Path to the outputs directory
        status_folder (str): Path to the status directory
    """
    def __init__(
        self,
        outputs_folder, 
        status_folder,
        long_term_db_target:LongTermDatabaseUploader,
        enabled_scrapers=None,
        enabled_file_types=None
        
    ):
        """
        Initialize the LongTermDatasetManager.
        
        Args:
            outputs_folder (str): Path to the outputs directory
            status_folder (str): Path to the status directory
            long_term_db_target (class): Class to use for remote database management
            enabled_scrapers (list, optional): List of enabled scrapers
            enabled_file_types (list, optional): List of enabled file types
        """
        self.when = now()
        self.enabled_scrapers = (
            "ALL" if not enabled_scrapers else ",".join(enabled_scrapers)
        )
        self.enabled_file_types = (
            "ALL" if not enabled_file_types else ",".join(enabled_file_types)
        )
        self.remote_database_manager = long_term_db_target
        self.outputs_folder = outputs_folder
        self.status_folder = status_folder


    def _read_parser_status(self):
        """
        Read and parse the parser status file.
        
        Returns:
            list: List of dictionaries containing file paths and descriptions
                  for successfully created files
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
                        "description": f"{len(entry['response']['files_to_process'])} XML files from type {entry['response']['files_types']} published by '{entry['store_enum']}'",
                    }
                )

        return descriptions

    def _read_scraper_status_files(self):
        """
        Read all scraper status files from the status directory.
        
        Returns:
            list: List of dictionaries containing file paths and descriptions
                  for each scraper status file
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
        Stage data for upload to the remote database.
        
        This method stages both the outputs folder and status folder,
        and increments the dataset version index.
        """
        self.remote_database_manager.stage(self.outputs_folder)
        self.remote_database_manager.stage(self.status_folder)        
        self.remote_database_manager.increase_index()

    def upload(self):
        """
        Upload staged data to the remote dataset.
        
        Creates a new version of the dataset with updated files and metadata.
        Includes parser status, scraper status, and processed files.
        
        Raises:
            ValueError: If the upload fails
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
                + self._read_parser_status()
                + self._read_scraper_status_files(),
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
        """
        Clean up temporary files and resources used during the upload process.
        """
        shutil.rmtree(self.outputs_folder,ignore_errors=True)
        shutil.rmtree(self.status_folder,ignore_errors=True)
        self.remote_database_manager.clean()
