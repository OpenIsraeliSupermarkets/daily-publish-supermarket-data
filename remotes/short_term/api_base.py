"""Base module for API-based database uploaders.

This module defines the base class for database uploaders that interact with APIs,
providing a consistent interface for different implementations.
"""


# pylint: disable=too-few-public-methods
class APIDatabaseUploader:
    """Base class for API-based database uploaders.

    This class defines the interface that all API database uploaders must implement.
    It provides methods for managing data storage, retrieval, and status tracking
    through API interactions.
    """

    def __init__(self, *_):
        """Initialize the API database uploader.

        Args:
            *_: Variable arguments (unused in base class)
        """

    def _insert_to_database(self, table_target_name, items):
        """Insert items into the database.

        Args:
            table_target_name (str): Name of the target table/collection
            items (list): List of items to insert
        """

    def _create_table(self, partition_id, table_name):
        """Create a new table/collection in the database.

        Args:
            partition_id (str): Field to use as partition key
            table_name (str): Name of the table/collection to create
        """

    def _clean_all_tables(self):
        """Delete all tables/collections in the database."""

    def _get_all_files_by_chain(self, chain: str, file_type=None):
        """Get all files associated with a specific chain.

        Args:
            chain (str): Chain identifier
            file_type (str, optional): Type of files to filter by

        Returns:
            list: List of files matching the criteria
        """

    def _get_content_of_file(self, table_name, file):
        """Retrieve content of a specific file.

        Args:
            table_name (str): Name of the table/collection
            file (str): File identifier

        Returns:
            list: List of items matching the file
        """

    def is_parser_updated(self) -> bool:
        """Check if the parser was updated recently.

        Returns:
            bool: True if parser was updated within last hour, False otherwise
        """
