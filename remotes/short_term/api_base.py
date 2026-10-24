"""Base module for API-based database uploaders.

This module defines the base class for database uploaders that interact with APIs,
providing a consistent interface for different implementations.
"""

import asyncio

from data_models.raw_schema import ParserStatus, ScraperStatus, list_all_dynamic_tables
from .row_stream import BufferedRowStream


# pylint: disable=too-few-public-methods
class ShortTermDatabaseUploader:
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

    def _insert_to_destinations(self, table_target_name, items):
        """Insert items into the database.

        Args:
            table_target_name (str): Name of the target table/collection
            items (list): List of items to insert
        """

    def stream_rows(self, push, on_file_stored=None):
        """Run ``push(stream)`` with a row stream into this database.

        ``push`` is an async function that calls ``stream.add_row`` and
        ``stream.end_file``. Returns the stream once every ended file is
        stored, with ``stored_files`` and ``failed_files`` filled in.

        Args:
            push: async callable taking a RowStream
            on_file_stored: called with each file name whose file_complete is stored
        """

        async def _run():
            stream = BufferedRowStream(self, on_file_stored=on_file_stored)
            try:
                await push(stream)
            finally:
                await stream.close()
            return stream

        return asyncio.run(_run())

    def _create_destinations(self, partition_id, table_name):
        """Create a new table/collection in the database.

        Args:
            partition_id (str): Field to use as partition key
            table_name (str): Name of the table/collection to create
        """

    def _clean_all_destinations(self):
        """Delete all tables/collections in the database."""

    def get_destinations_content(self, table_name, filter=None):
        """Get all content of all tables/collections in the database.

        Args:
            table_name (str): Name of the table/collection
            filter (dict, optional): Filter to apply to the content

        Returns:
            list: List of all items in the table/collection
        """

    def _is_collection_updated(
        self, collection_name: str, seconds: int = 10800
    ) -> bool:
        """Check if the parser was updated recently.

        Args:
            collection_name (str): Name of the collection
            seconds (int, optional): Time window in seconds to check for updates.
                                   Defaults to 10800 (3 hours).

        Returns:
            bool: True if parser was updated within specified time window, False otherwise
        """

    def _list_destinations(self):
        """List all tables/collections in the database.

        Returns:
            list[str]: List of table/collection names in the database

        Raises:
            NotImplementedError: This is an abstract method that must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement _list_destinations()")

    def restart_database(
        self, enabled_scrapers: list[str], enabled_file_types: list[str]
    ):
        """Clean and recreate all tables in the database.

        This function drops all existing tables and recreates them with their original structure.
        Only clean_all_source_data and reload_complete_api should call it.
        """
        self._clean_all_destinations()
        self.create_database(enabled_scrapers, enabled_file_types)

    def create_database(
        self, enabled_scrapers: list[str], enabled_file_types: list[str]
    ):
        """Create the tables that don't exist yet; existing tables keep their rows."""
        self._create_destinations(
            ParserStatus.get_index(), ParserStatus.get_table_name()
        )
        self._create_destinations(
            ScraperStatus.get_index(), ScraperStatus.get_table_name()
        )
        self._create_destinations(ParserStatus.get_index(), "GlobalParserStatus")
        self._create_destinations(ScraperStatus.get_index(), "GlobalScraperStatus")
        self._create_destinations(
            ScraperStatus.get_index(), "VerifiedScraperDownloads"
        )
        self._create_destinations(
            ParserStatus.get_index(), "VerifiedParserDownloads"
        )
        for table in list_all_dynamic_tables(enabled_scrapers, enabled_file_types):
            self._create_destinations(table.get_index(), table.get_table_name())
