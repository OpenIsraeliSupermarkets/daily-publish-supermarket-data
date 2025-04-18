"""Module for handling Kaggle dataset uploads and management.

This module provides functionality to upload, update and manage datasets on Kaggle,
specifically designed for supermarket data management.
"""

import os
import pytz
import logging
import shutil
import json
from datetime import datetime, timedelta
from .base import LongTermDatabaseUploader

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
            )[0]
            return (
                datetime.now(tz=pytz.utc)
                - dataset_info.lastUpdated.replace(tzinfo=pytz.utc)
            ) < timedelta(seconds=seconds)
        except Exception as e:  # pylint: disable=W0718
            logging.error("Error checking Kaggle dataset update time: %s", str(e))
            return False
