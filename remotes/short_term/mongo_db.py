"""MongoDB implementation of the database uploader.

This module provides functionality for uploading and managing data in MongoDB,
handling large integers, collection management, and status tracking.
"""

import logging
import os
import re
from datetime import datetime, timedelta

import pymongo

from .api_base import APIDatabaseUploader


class MongoDbUploader(APIDatabaseUploader):
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

    def pre_process(self, item):
        """Convert large integers to strings to avoid MongoDB limitations.

        Args:
            item: The item to preprocess (can be dict, list, or primitive type)

        Returns:
            The preprocessed item with large integers converted to strings
        """
        if isinstance(item, list):
            return [self.pre_process(i) for i in item]
        if isinstance(item, dict):
            return {k: self.pre_process(v) for k, v in item.items()}
        if isinstance(item, int) and (item > 2**63 - 1 or item < -(2**63)):
            return str(item)
        return item

    def _insert_to_database(self, table_target_name, items):
        """Insert items into a MongoDB collection with error handling.

        Args:
            table_target_name (str): Name of the target collection
            items (list): List of items to insert
        """
        if not items:
            return

        logging.info("Pushing to table %s, %d items", table_target_name, len(items))
        collection = self.db[table_target_name]
        processed_items = list(map(self.pre_process, items))

        try:
            collection.insert_many(processed_items, ordered=False)
            logging.info("Successfully inserted %d records to MongoDB", len(items))
        except pymongo.errors.BulkWriteError as e:
            logging.warning("Bulk insert failed, trying individual inserts: %s", str(e))
            successful_records = 0
            for record in processed_items:
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

    def _create_table(self, partition_id, table_name):
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

    def _clean_all_tables(self):
        """Drop all collections in the database."""
        for collection in self.db.list_collection_names():
            self.db[collection].drop()
        logging.info("All collections deleted successfully!")

    def _get_all_files_by_chain(self, chain: str, file_type=None):
        """Get all files associated with a specific chain.

        Args:
            chain (str): Chain identifier
            file_type (str, optional): Type of files to filter by

        Returns:
            list: List of files matching the criteria
        """
        collection = self.db["ParserStatus"]
        files = []

        filter_condition = f".*{re.escape(chain)}.*"
        if file_type is not None:
            filter_condition = f".*{re.escape(file_type)}.*{re.escape(chain)}.*"

        for doc in collection.find({"index": {"$regex": filter_condition}}):
            if "response" in doc and "files_to_process" in doc["response"]:
                files.extend(doc["response"]["files_to_process"])
        return files

    def _get_content_of_file(self, table_name, file):
        """Retrieve content of a specific file from a collection.

        Args:
            table_name (str): Name of the collection
            file (str): File identifier

        Returns:
            list: List of documents matching the file
        """
        collection = self.db[table_name]
        results = []
        for obj in collection.find({"file_name": file}):
            # Convert ObjectId to dict manually
            obj_dict = {k: v for k, v in obj.items() if k != "_id"}
            results.append(obj_dict)
        return results

    def is_parser_updated(self, hours: int = 3) -> bool:
        """Check if the parser status was updated recently.

        Args:
            hours (int, optional): Number of hours to look back. Defaults to 3.

        Returns:
            bool: True if parser was updated within specified hours, False otherwise
        """
        try:
            collection = self.db["ParserStatus"]
            latest_doc = collection.find_one(sort=[("_id", pymongo.DESCENDING)])

            if not latest_doc:
                return False

            last_modified = latest_doc["_id"].generation_time
            return (datetime.now(last_modified.tzinfo) - last_modified) < timedelta(
                hours=hours
            )

        except pymongo.errors.PyMongoError as e:
            logging.error("Error checking MongoDB ParserStatus update time: %s", str(e))
            return False
