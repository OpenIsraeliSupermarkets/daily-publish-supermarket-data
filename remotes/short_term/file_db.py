"""Document-based database implementation for testing.

This module provides a file-system based implementation of a document database,
primarily used for testing and development purposes.
"""

import re
import os
import json
from utils import Logger
from datetime import datetime, timedelta
from .api_base import ShortTermDatabaseUploader


class DummyDocumentDbUploader(ShortTermDatabaseUploader):
    """File-system based implementation of a document database.

    This class implements a simple document database using the file system,
    useful for testing and development without requiring a real database.
    Each table is a directory and each document is stored as a JSON file.
    """

    def __init__(self, db_path):
        """Initialize the document database.

        Args:
            db_path (str): Base path for storing the database files
        """
        self.db_path = os.path.join(db_path, "document_db")
        os.makedirs(self.db_path, exist_ok=True)
        self._load_tables_ids()

    def _load_tables_ids(self):
        """Load table ID mappings from storage."""
        tables_ids_path = os.path.join(self.db_path, "tables_ids.json")
        if os.path.exists(tables_ids_path):
            with open(tables_ids_path, "r", encoding="utf-8") as f:
                self.tables_ids = json.load(f)
        else:
            self.tables_ids = {}

    def _save_tables_ids(self):
        """Save table ID mappings to storage."""
        tables_ids_path = os.path.join(self.db_path, "tables_ids.json")
        with open(tables_ids_path, "w", encoding="utf-8") as f:
            json.dump(self.tables_ids, f, indent=4, ensure_ascii=False)

    def _clean_meta_data(self):
        """Remove metadata files."""
        tables_ids_path = os.path.join(self.db_path, "tables_ids.json")
        if os.path.exists(tables_ids_path):
            os.remove(tables_ids_path)

    def _insert_to_destinations(self, table_target_name, items):
        """Insert items into a table.

        Args:
            table_target_name (str): Name of the target table
            items (list): List of items to insert
        """
        table_path = os.path.join(self.db_path, table_target_name)
        os.makedirs(table_path, exist_ok=True)

        id_name = self.tables_ids[table_target_name]
        for item in items:
            item_id = item.get(id_name, None)
            if item_id is None:
                raise ValueError(f"Item {item} does not have an ID")

            file_path = os.path.join(table_path, f"{item_id}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(item, f, indent=4, ensure_ascii=False)

    def _create_destinations(self, partition_id, table_name):
        """Create a new table directory.

        Args:
            partition_id (str): Field to use as partition key
            table_name (str): Name of the table to create
        """
        table_path = os.path.join(self.db_path, table_name)
        os.makedirs(table_path, exist_ok=True)
        self.tables_ids[table_name] = partition_id
        self._save_tables_ids()
        Logger.info("Created table: %s", table_name)

    def _clean_all_destinations(self):
        """Remove all tables and their contents."""
        self._clean_meta_data()
        if os.path.exists(self.db_path):
            for table_name in os.listdir(self.db_path):
                table_path = os.path.join(self.db_path, table_name)
                if os.path.isdir(table_path):
                    for file in os.listdir(table_path):
                        os.remove(os.path.join(table_path, file))
                    os.rmdir(table_path)
        Logger.info("All tables deleted successfully!")

    def _is_collection_updated(
        self, collection_name: str, seconds: int = 10800
    ) -> bool:
        """Check if the parser was updated within the specified time window.

        Args:
            seconds (int): Time window in seconds (default: 10800 seconds = 3 hours)

        Returns:
            bool: True if parser was updated within the time window
        """
        try:
            parser_path = os.path.join(self.db_path, collection_name)
            if not os.path.exists(parser_path):
                return False

            now = datetime.now()
            last_modified = None

            for filename in os.listdir(parser_path):
                file_path = os.path.join(parser_path, filename)
                if os.path.isfile(file_path):
                    mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if last_modified is None or mtime > last_modified:
                        last_modified = mtime

            if last_modified is None:
                return False

            return (now - last_modified) < timedelta(seconds=seconds)

        except Exception as e:  # pylint: disable=W0718
            Logger.error(
                "Error checking DummyDocumentDb ParserStatus update time: %s", str(e)
            )
            return False

    def _list_destinations(self):
        """List all tables/collections in the database.

        Returns:
            list[str]: List of table/collection names in the database
        """
        tables = []
        for item in os.listdir(self.db_path):
            path = os.path.join(self.db_path, item)
            if os.path.isdir(path) and item != "__pycache__":
                tables.append(item)
        return tables

    def get_destinations_content(self, table_name, filter=None):
        """Get all content of a specific table.

        Args:
            table_name (str): Name of the table

        Returns:
            list: List of all documents in the table
        """
        table_path = os.path.join(self.db_path, table_name)
        if not os.path.exists(table_path):
            Logger.error("Table '%s' does not exist", table_name)
            return []

        content = []
        for filename in os.listdir(table_path):
            file_path = os.path.join(table_path, filename)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                    if filter is not None:
                        if all(
                            (
                                isinstance(value, dict)
                                and "$regex" in value
                                and re.match(value["$regex"], str(data.get(key)))
                            )
                            or (data.get(key) == value)
                            for key, value in filter.items()
                        ):
                            content.append(data)
                    else:
                        content.append(data)
            except Exception as e:  # pylint: disable=W0718
                Logger.error("Error reading file %s: %s", file_path, str(e))
        return content
