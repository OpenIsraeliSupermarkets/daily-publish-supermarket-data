"""Base module for remote database uploaders.

This module defines the abstract base class for all remote database uploaders,
ensuring consistent interface across different implementations.
"""

from abc import ABC, abstractmethod


class LongTermDatabaseUploader(ABC):
    """Abstract base class for uploading data to remote databases.

    This class defines the interface that all remote database uploaders must implement.
    It provides methods for managing dataset versions, uploading data, and checking
    update status.
    """
    NO_INDEX = -1

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

    @abstractmethod
    def clean(self):
        """Clean up any temporary files or resources.

        This method should handle cleanup of any temporary files, resources
        created during the upload process and the data that was uploaded.
        """

    @abstractmethod
    def was_updated_in_last(self, seconds: int = 24*60*60) -> bool:
        """Check if the remote dataset was updated within specified hours.

        Args:
            seconds (int, optional): Number of seconds to look back. Defaults to 24*60*60.

        Returns:
            bool: True if the dataset was updated within specified hours,
                 False otherwise
        """
            
    def _read_index(self,index):
        if index is None:
            return self.NO_INDEX
        return max(map(int, index.keys()))
           
    def _increase_index(self,index):
        """Increase the index of the dataset.
        """
        if index is None or index == -1:
            index = {self.NO_INDEX + 1: self.when.strftime("%Y-%m-%d %H:%M:%S")}
        else:   
            index[str(max(map(int, index.keys())) + 1)] = self.when.strftime("%Y-%m-%d %H:%M:%S")
        return index
