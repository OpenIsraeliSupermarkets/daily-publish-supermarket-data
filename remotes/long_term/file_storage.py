"""Module for handling file-based storage operations.

This module provides functionality for storing and managing data in a local file system,
primarily used for testing and development purposes.
"""

import os
import logging
import shutil
from datetime import datetime
from ..utils import was_updated_within_hours
from .base import LongTermDatabaseUploader
import json


class DummyFileStorage(LongTermDatabaseUploader):
    """A dummy implementation of remote storage using local file system.

    This class implements the RemoteDatabaseUploader interface but stores data
    locally instead of uploading to a remote service. Useful for testing and
    development purposes.
    """

    def __init__(
        self,
        dataset_path="/",
        when=datetime.now(),
        dataset_remote_name="israeli-supermarkets-2024",
    ):
        """Initialize the dummy file storage.

        Args:
            dataset_path (str): Path to the dataset files
            when (datetime): Timestamp for the dataset
            dataset_remote_name (str): Name to use for the local storage directory
        """
        self.dataset_remote_name = dataset_remote_name
        self.dataset_path = dataset_path
        self.when = when

    def get_current_index(self):
        """Get the current index of the dataset.

        Returns:
            int: The current index of the dataset
        """
        if os.path.exists(os.path.join(self.dataset_remote_name, "index.json")):
            with open(os.path.join(self.dataset_remote_name, "index.json"), "r") as f:
                index = json.load(f)
            return index["index"]
        else:
            return 0

    def increase_index(self):
        """Write an index file with value 1."""
        index_path = os.path.join(self.dataset_remote_name, "index.json")
        os.makedirs(self.dataset_remote_name, exist_ok=True)
        with open(index_path, "w") as f:
            json.dump({"index": 1}, f)

    def upload_to_dataset(self, message):
        """Copy files to the local storage directory.

        Args:
            message (str): Message to log with the upload operation
        """
        logging.info(
            "Uploading dataset '%s' to remote database, message %s",
            self.dataset_remote_name,
            message,
        )
        os.makedirs(self.dataset_remote_name, exist_ok=True)
        for filename in os.listdir(self.dataset_path):
            file_path = os.path.join(self.dataset_path, filename)
            if os.path.isfile(file_path):
                shutil.copy(file_path, self.dataset_remote_name)

    def clean(self):
        """Clean up any temporary files."""
        return None

    def was_updated_in_last(self, hours: int = 24) -> bool:
        """Check if any files in the storage were updated within specified hours.

        Args:
            hours (int, optional): Number of hours to look back. Defaults to 24.

        Returns:
            bool: True if any file was updated within specified hours, False otherwise
        """
        return was_updated_within_hours(self.dataset_remote_name, hours)
