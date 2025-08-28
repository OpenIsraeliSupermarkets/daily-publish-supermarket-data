"""Module for handling file-based storage operations.

This module provides functionality for storing and managing data in a local file system,
primarily used for testing and development purposes.
"""

import os
import glob
from utils import Logger
import shutil
from ..utils import was_updated_within_seconds
from .base import LongTermDatabaseUploader
import json
import pandas as pd
from il_supermarket_scarper import DumpFolderNames


class DummyFileStorage(LongTermDatabaseUploader):
    """A dummy implementation of remote storage using local file system.

    This class implements the RemoteDatabaseUploader interface but stores data
    locally instead of uploading to a remote service. Useful for testing and
    development purposes.
    """

    def __init__(
        self,
        dataset_path,
        dataset_remote_path,
        when,
    ):
        """Initialize the dummy file storage.

        Args:
            dataset_path (str): Path to the dataset files
            when (datetime): Timestamp for the dataset
            dataset_remote_name (str): Name to use for the local storage directory
        """
        super().__init__(dataset_path, when)
        self.dataset_remote_path = dataset_remote_path
        self.when = when

    def _load_index(self):
        index = None
        if os.path.exists(os.path.join(self.dataset_path, "index.json")):
            with open(os.path.join(self.dataset_path, "index.json"), "r") as f:
                index = json.load(f)
        return index

    def get_current_index(self):
        """Get the current index of the dataset.

        Returns:
            int: The current index of the dataset
        """
        index = self._load_index()
        return self._read_index(index)

    def increase_index(self):
        """Write an index file with value 1."""
        index = self._load_index()
        index = self._increase_index(index)

        os.makedirs(self.dataset_path, exist_ok=True)
        with open(os.path.join(self.dataset_path, "index.json"), "w") as f:
            json.dump(index, f)

    def upload_to_dataset(self, message, **additional_metadata):
        """Copy files to the local storage directory.

        Args:
            message (str): Message to log with the upload operation
        """
        Logger.info(
            "Uploading dataset '%s' to remote database, message %s",
            self.dataset_remote_path,
            message,
        )
        os.makedirs(self.dataset_remote_path, exist_ok=True)
        for filename in os.listdir(self.dataset_path):
            file_path = os.path.join(self.dataset_path, filename)
            if os.path.isfile(file_path):
                shutil.copy(file_path, self.dataset_remote_path)

    def was_updated_in_last(self, seconds: int = 24 * 60 * 60) -> bool:
        """Check if any files in the storage were updated within specified hours.

        Args:
            seconds (int, optional): Number of seconds to look back. Defaults to 24*60*60.

        Returns:
            bool: True if any file was updated within specified hours, False otherwise
        """
        return was_updated_within_seconds(self.dataset_remote_path, seconds)

    def list_files(self, chain=None, extension=None):
        files = glob.glob(
            os.path.join(
                self.dataset_remote_path, self._build_pattern(chain, extension)
            )
        )
        return [os.path.basename(f) for f in files]

    def get_file_content(self, file_name):
        if file_name.endswith(".json"):
            with open(
                os.path.join(self.dataset_remote_path, file_name), "r", encoding="utf-8"
            ) as file:
                return json.load(file)
        elif file_name.endswith(".csv"):
            # Read and return the CSV file as a DataFrame
            return pd.read_csv(os.path.join(self.dataset_remote_path, file_name))
        else:
            with open(os.path.join(self.dataset_remote_path, file_name), "r") as file:
                return file.read()
