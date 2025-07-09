"""Module for handling Kaggle dataset uploads and management.

This module provides functionality to upload, update and manage datasets on Kaggle,
specifically designed for supermarket data management.
"""

import time
import os
import re
import pytz
import logging
import shutil
import json
import pandas as pd
from datetime import datetime, timedelta
from .base import LongTermDatabaseUploader
from il_supermarket_scarper import DumpFolderNames

KAGGLE_API_AVAILABLE = None
try:
    from kaggle.api.kaggle_api_extended import KaggleApi
    from kaggle.rest import ApiException
except IOError as e:
    KAGGLE_API_AVAILABLE = e


class KaggleUploader(LongTermDatabaseUploader):
    """Handles uploading and managing datasets on Kaggle.

    This class provides methods to upload data to Kaggle, manage dataset versions,
    and check update status of datasets.
    """

    def __init__(self, dataset_path, dataset_remote_name, when):
        """Initialize the Kaggle uploader.

        Args:
            dataset_path (str): Path to the dataset files
            when (datetime): Timestamp for the dataset
            dataset_remote_name (str): Name of the remote Kaggle dataset
        """
        super().__init__(dataset_path, when)
        self.dataset_remote_name = dataset_remote_name
        self.when = when

        if KAGGLE_API_AVAILABLE is not None:
            raise ImportError(
                "Fail to use kaggle api, message: \n%s" % KAGGLE_API_AVAILABLE
            )

        self.api = KaggleApi()
        self.api.authenticate()

    def _sync_n_load_index(self):
        """Sync the index of the dataset."""
        try:
            if not os.path.exists(os.path.join(self.dataset_path, "index.json")):
                self.api.dataset_download_cli(
                    f"erlichsefi/{self.dataset_remote_name}",
                    file_name="index.json",
                    force=True,
                    path=self.dataset_path,
                )
            else:
                logging.warn("Index file already exists")

            with open(
                os.path.join(self.dataset_path, "index.json"), "r", encoding="utf-8"
            ) as file:
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
        return self._read_index(index)

    def increase_index(self):
        """Download and update the dataset index from Kaggle."""
        index = self._sync_n_load_index()
        index = self._increase_index(index)

        os.makedirs(self.dataset_path, exist_ok=True)
        with open(
            os.path.join(self.dataset_path, "index.json"), "w+", encoding="utf-8"
        ) as file:
            json.dump(index, file)

    def upload_to_dataset(self, message, **additional_metadata):
        """Upload a new version of the dataset.

        Args:
            message (str): Version notes for the upload
        """
        os.makedirs(self.dataset_path, exist_ok=True)
        with open(
            os.path.join(self.dataset_path, "dataset-metadata.json"), "w"
        ) as file:
            json.dump(
                {"id": f"erlichsefi/{self.dataset_remote_name}", **additional_metadata},
                file,
            )
        self.api.dataset_create_version(
            folder=self.dataset_path,
            version_notes=message,
            delete_old_versions=False,
        )
        time.sleep(3)  # wait for kaggle to process the request.

    def clean(self):
        """Clean up temporary files."""
        shutil.rmtree(self.dataset_path)
        super().clean()

    def was_updated_in_last(self, seconds: int = 24 * 60 * 60) -> bool:
        """Check if the dataset was updated within specified hours.

        Args:
            seconds (int, optional): Number of seconds to look back. Defaults to 24*60*60.

        Returns:
            bool: True if updated within specified hours, False otherwise
        """
        try:
            dataset_info = self.api.dataset_list(
                user="erlichsefi", search=self.dataset_remote_name
            )
            dataset_info = list(
                filter(lambda x: x.title == self.dataset_remote_name, dataset_info)
            )[0]
            return (
                datetime.now(tz=pytz.utc)
                - dataset_info.lastUpdated.replace(tzinfo=pytz.utc)
            ) < timedelta(seconds=seconds)
        except Exception as e:  # pylint: disable=W0718
            logging.error("Error checking Kaggle dataset update time: %s", str(e))
            return False

    def list_files(self, chain=None, extension=None):
        """List all CSV files in the dataset.

        Args:
            chain (str, optional): Filter files by chain name. Defaults to None.

        Returns:
            list: List of file paths in the dataset
        """
        # Download files if needed
        try:

            # Download all dataset files if not already present
            page_token = None
            collected_files = []
            while True:
                files = self.api.dataset_list_files(
                    f"erlichsefi/{self.dataset_remote_name}",
                    page_token=page_token,
                )
                collected_files.extend([file.name for file in files.files])
                if files.nextPageToken == "":
                    break
                page_token = files.nextPageToken

            # Filter by chain if specified
            if chain is not None or extension is not None:
                pattern = self._build_pattern(chain, extension)
                # Use glob-style pattern matching to filter files
                collected_files = [
                    f
                    for f in collected_files
                    if re.match(pattern.replace("*", ".*"), f)
                ]
            return collected_files
        except ApiException as e:
            logging.error("Error listing files from Kaggle: %s", str(e))
            return []

    def get_file_content(self, file_name):
        """Get the content of a specific file from the dataset.

        Args:
            file_name (str): Name of the file to retrieve

        Returns:
            pandas.DataFrame: Content of the file as a DataFrame
        """
        try:
            # Ensure the file exists locally
            if not os.path.exists(file_name):
                # Download specific file if it doesn't exist
                self.api.dataset_download_file(
                    f"erlichsefi/{self.dataset_remote_name}", file_name=file_name
                )

            if file_name.endswith(".json"):
                with open(file_name, "r", encoding="utf-8") as file:
                    return json.load(file)
            elif file_name.endswith(".csv"):
                # Read and return the CSV file as a DataFrame
                return pd.read_csv(file_name)
            else:
                with open(file_name, "r") as file:
                    return file.read()
        except ApiException as e:
            logging.error("Error getting file content from Kaggle: %s", str(e))
            raise e
        finally:
            if os.path.exists(file_name):
                os.remove(file_name)
