"""Module for handling Kaggle dataset uploads and management.

This module provides functionality to upload, update and manage datasets on Kaggle,
specifically designed for supermarket data management.
"""

import os
import logging
import json
from datetime import datetime, timedelta
from .base import LongTermDatabaseUploader

KAGGLE_API_AVAILABLE = False
try:
    from kaggle.api.kaggle_api_extended import KaggleApi
    from kaggle.rest import ApiException
    KAGGLE_API_AVAILABLE = True
except IOError:
    pass


class KaggleUploader(LongTermDatabaseUploader):
    """Handles uploading and managing datasets on Kaggle.

    This class provides methods to upload data to Kaggle, manage dataset versions,
    and check update status of datasets.
    """

    def __init__(
        self,
        dataset_path="",
        when=datetime.now(),
        dataset_remote_name="israeli-supermarkets-2024",
    ):
        """Initialize the Kaggle uploader.

        Args:
            dataset_path (str): Path to the dataset files
            when (datetime): Timestamp for the dataset
            dataset_remote_name (str): Name of the remote Kaggle dataset
        """

        self.dataset_remote_name = dataset_remote_name
        self.dataset_path = dataset_path
        self.when = when

        if not KAGGLE_API_AVAILABLE:
            raise ImportError(
                "kaggle-api is not installed. Please install it using 'pip install kaggle-api'."
            )

        self.api = KaggleApi()
        self.api.authenticate()

    def _sync_n_load_index(self):
        """Sync the index of the dataset.
        """
        try:
            if not os.path.exists(os.path.join(self.dataset_path, "index.json")):
                self.api.dataset_download_cli(
                    f"erlichsefi/{self.dataset_remote_name}", file_name="index.json", force=True,
                    path=self.dataset_path
                )
            else:
                logging.warn("Index file already exists")
                
            with open(os.path.join(self.dataset_path, "index.json"), "r", encoding="utf-8") as file:
                index = json.load(file)
            return index
        except ApiException as e: 
            if e.reason == "Not Found":
                return None
            raise Exception("Error connection to kaggle")

    def get_current_index(self):
        """Get the current index of the dataset.

        Returns:
            int: The current index of the dataset
        """
        
        index = self._sync_n_load_index()
        if index is None:
            return self.NO_INDEX
        return index[max(map(int, index.keys()))]
           

    def increase_index(self):
        """Download and update the dataset index from Kaggle."""
        index = self._sync_n_load_index()
        if index is None:
            index = {self.NO_INDEX + 1: self.when.strftime("%Y-%m-%d %H:%M:%S")}
        else:
            index[max(map(int, index.keys())) + 1] = self.when.strftime("%Y-%m-%d %H:%M:%S")

        with open(
            os.path.join(self.dataset_path, "index.json"), "w+", encoding="utf-8"
        ) as file:
            json.dump(index, file)

    def upload_to_dataset(self, message):
        """Upload a new version of the dataset.

        Args:
            message (str): Version notes for the upload
        """
        self.api.dataset_create_version(
            folder=self.dataset_path,
            version_notes=message,
            delete_old_versions=False,
        )

    def clean(self):
        """Clean up temporary files."""
        if os.path.exists("index.json"):
            os.remove("index.json")

    def was_updated_in_last(self, hours: int = 24) -> bool:
        """Check if the dataset was updated within specified hours.

        Args:
            hours (int, optional): Number of hours to look back. Defaults to 24.

        Returns:
            bool: True if updated within specified hours, False otherwise
        """
        try:
            dataset_info = self.api.dataset_list(
                search=f"erlichsefi/{self.dataset_remote_name}"
            )[0]
            return (datetime.now() - dataset_info.lastUpdated) < timedelta(hours=hours)
        except Exception as e:  # pylint: disable=W0718
            logging.error("Error checking Kaggle dataset update time: %s", str(e))
            return False
