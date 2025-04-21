"""Base module for remote database uploaders.

This module defines the abstract base class for all remote database uploaders,
ensuring consistent interface across different implementations.
"""

from abc import ABC, abstractmethod
import shutil
import os
import datetime


class LongTermDatabaseUploader(ABC):
    """Abstract base class for uploading data to remote databases.

    This class defines the interface that all remote database uploaders must implement.
    It provides methods for managing dataset versions, uploading data, and checking
    update status.
    """

    NO_INDEX = -1

    def __init__(self, dataset_path: str, when: datetime.datetime):
        self.dataset_path = dataset_path
        self.when = when

    @abstractmethod
    def increase_index(self):
        """Increment the dataset version index.

        This method should handle the logic for managing dataset versions
        and incrementing the version index as needed.
        """

    @abstractmethod
    def get_current_index(self):
        """Get the current index of the dataset.

        Returns:
            int: The current index of the dataset
        """

    @abstractmethod
    def upload_to_dataset(self, message):
        """Upload data to the remote dataset.

        Args:
            message (str): A message describing the upload/version
        """

    def clean(self):
        """Clean up any temporary files or resources.

        This method should handle cleanup of any temporary files, resources
        created during the upload process and the data that was uploaded.
        """
        shutil.rmtree(self.dataset_path, ignore_errors=True)

    @abstractmethod
    def was_updated_in_last(self, seconds: int = 24 * 60 * 60) -> bool:
        """Check if the remote dataset was updated within specified hours.

        Args:
            seconds (int, optional): Number of seconds to look back. Defaults to 24*60*60.

        Returns:
            bool: True if the dataset was updated within specified hours,
                 False otherwise
        """

    @abstractmethod
    def list_files(self, chain=None, extension=None):
        """List all files in the remote dataset.

        Args:
            chain (str, optional): Filter files by chain name. Defaults to None.
            extension (str, optional): Filter files by extension. Defaults to None.
        Returns:
            list: List of file paths in the dataset
        """

    @abstractmethod
    def get_file_content(self, file_name):
        """Get the content of a specific file from the dataset.

        Args:
            file_name (str): Name of the file to retrieve

        Returns:
            pandas.DataFrame: Content of the file as a DataFrame
        """

    def stage(self, folder_or_file):
        """Stage a folder for upload to Kaggle.

        Args:
            folder_or_file (str): Path to the folder or file to stage
        """
        if os.path.isdir(folder_or_file):
            shutil.copytree(folder_or_file, self.dataset_path, dirs_exist_ok=True)
        else:
            shutil.copy2(folder_or_file, self.dataset_path)

    def _read_index(self, index):
        if index is None:
            return self.NO_INDEX
        return max(map(int, index.keys()))

    def _increase_index(self, index):
        """Increase the index of the dataset."""
        if index is None or index == -1:
            index = {self.NO_INDEX + 1: self.when.strftime("%Y-%m-%d %H:%M:%S")}
        else:
            index[str(max(map(int, index.keys())) + 1)] = self.when.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        return index


    def _build_pattern(self, chain=None, extension=None):
        pattern = "*"
        if chain:
            pattern = f"*{chain.lower()}"
        if extension:
            pattern = f"{pattern}.{extension}"
        return pattern