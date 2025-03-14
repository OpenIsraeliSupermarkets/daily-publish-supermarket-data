"""Base module for remote database uploaders.

This module defines the abstract base class for all remote database uploaders,
ensuring consistent interface across different implementations.
"""

from abc import ABC, abstractmethod


class RemoteDatabaseUploader(ABC):
    """Abstract base class for uploading data to remote databases.

    This class defines the interface that all remote database uploaders must implement.
    It provides methods for managing dataset versions, uploading data, and checking
    update status.
    """

    @abstractmethod
    def increase_index(self):
        """Increment the dataset version index.

        This method should handle the logic for managing dataset versions
        and incrementing the version index as needed.
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

        This method should handle cleanup of any temporary files or resources
        created during the upload process.
        """

    @abstractmethod
    def was_updated_in_last_24h(self, hours: int = 24) -> bool:
        """Check if the remote dataset was updated within specified hours.

        Args:
            hours (int, optional): Number of hours to look back. Defaults to 24.

        Returns:
            bool: True if the dataset was updated within specified hours,
                 False otherwise
        """
