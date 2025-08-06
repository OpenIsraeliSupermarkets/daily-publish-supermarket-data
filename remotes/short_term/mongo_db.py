"""MongoDB implementation of the database uploader.

This module provides functionality for uploading and managing data in MongoDB,
handling large integers, collection management, and status tracking.
"""

import logging
import os
import re
from datetime import datetime, timedelta

import pymongo

from .api_base import ShortTermDatabaseUploader


class MongoDbUploader(ShortTermDatabaseUploader):
    """MongoDB implementation for storing and managing supermarket data.

    This class handles all MongoDB-specific operations including data preprocessing,
    collection management, and status tracking. It includes special handling for
    large integers and bulk operations.
    """

    def __init__(self, mongodb_uri=None):
        """Initialize MongoDB connection.

        Args:
            mongodb_uri (str, optional): MongoDB connection URI. If not provided,
                                       uses environment variable or default.
        """
        uri = mongodb_uri or os.getenv(
            "MONGODB_URI", "mongodb://host.docker.internal:27017"
        )
        self.client = pymongo.MongoClient(uri)
        self.db = self.client.supermarket_data
        self._test_connection()
        
    def _test_connection(self):
        """Test the connection to the MongoDB database.
        """
        try:
            self.client.admin.command('ping')
            logging.info("Successfully connected to MongoDB")   
        except pymongo.errors.PyMongoError as e:
            logging.error("Error connecting to MongoDB: %s", str(e))
            raise e
            
    def _insert_to_destinations(self, table_target_name, items):
        """Insert items into a MongoDB collection with error handling.

        Args:
            table_target_name (str): Name of the target collection
            items (list): List of items to insert
        """
        if not items:
            return

        logging.info("Pushing to table %s, %d items", table_target_name, len(items))
        collection = self.db[table_target_name]

        try:
            collection.insert_many(items, ordered=False)
            logging.info("Successfully inserted %d records to MongoDB", len(items))
        except pymongo.errors.BulkWriteError as e:
            logging.warning("Bulk insert failed, trying individual inserts: %s", str(e))
            successful_records = 0
            for record in items:
                if "_id" in record:  #
                    try:
                        collection.insert_one(record)
                        successful_records += 1
                    except pymongo.errors.PyMongoError as inner_e:
                        logging.error("Failed to insert record: %s", str(inner_e))
            logging.info(
                "Successfully inserted %d/%d records individually",
                successful_records,
                len(items),
            )

    def _create_destinations(self, partition_id, table_name):
        """Create a new collection with an index.

        Args:
            partition_id (str): Field to use as partition key
            table_name (str): Name of the collection to create
        """
        logging.info("Creating collection: %s", table_name)
        try:
            self.db.create_collection(table_name)
            self.db[table_name].create_index(
                [(partition_id, pymongo.ASCENDING)], unique=True
            )
        except pymongo.errors.PyMongoError as e:
            logging.error("Error creating collection: %s", str(e))

    def _clean_all_destinations(self):
        """Drop all collections in the database."""
        for collection in self.db.list_collection_names():
            self.db[collection].drop()
        logging.info("All collections deleted successfully!")

    def _is_collection_updated(
        self, collection_name: str, seconds: int = 10800
    ) -> bool:
        """Check if the parser status was updated recently.

        Args:
            seconds (int, optional): Number of seconds to look back. Defaults to 10800 (3 hours).

        Returns:
            bool: True if parser was updated within specified time window, False otherwise
        """
        try:
            collection = self.db[collection_name]
            latest_doc = collection.find_one(sort=[("_id", pymongo.DESCENDING)])

            if not latest_doc:
                return False

            last_modified = latest_doc["_id"].generation_time
            return (datetime.now(last_modified.tzinfo) - last_modified) < timedelta(
                seconds=seconds
            )

        except pymongo.errors.PyMongoError as e:
            logging.error("Error checking MongoDB ParserStatus update time: %s", str(e))
            return False

    def _list_destinations(self):
        """List all tables/collections in the database.

        Returns:
            list[str]: List of table/collection names in the database
        """
        return self.db.list_collection_names()

    def get_destinations_content(self, table_name, filter=None):
        """Get all content of a specific table.

        Args:
            table_name (str): Name of the table/collection

        Returns:
            list: List of all documents in the collection
        """
        try:
            return list(self.db[table_name].find(filter, {"_id": 0}))
        except pymongo.errors.PyMongoError as e:
            logging.error(
                "Error retrieving documents from collection %s: %s", table_name, str(e)
            )
            return []
