"""DynamoDB implementation of the database uploader.

This module provides functionality for uploading and managing data in DynamoDB,
handling data types, table management, and status tracking.
"""

import logging
from datetime import datetime, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from .api_base import APIDatabaseUploader


class DynamoDbUploader(APIDatabaseUploader):
    """DynamoDB implementation for storing and managing supermarket data.

    This class handles all DynamoDB-specific operations including data preprocessing,
    table management, and status tracking. It includes special handling for
    floating point numbers and batch operations.
    """

    def __init__(self, region_name="us-east-1"):
        """Initialize DynamoDB connection.

        Args:
            region_name (str): AWS region name, defaults to us-east-1
        """
        self.dynamodb = boto3.resource("dynamodb", region_name=region_name)
        self.dynamodb_client = boto3.client("dynamodb", region_name=region_name)

    def pre_process(self, item):
        """Convert data types to DynamoDB compatible formats.

        Args:
            item: The item to preprocess (can be dict, list, or primitive type)

        Returns:
            The preprocessed item with appropriate data types for DynamoDB
        """
        if isinstance(item, list):
            return [self.pre_process(i) for i in item]
        if isinstance(item, dict):
            return {k: self.pre_process(v) for k, v in item.items()}
        if isinstance(item, float):
            return Decimal(str(item))
        return item

    def _insert_to_database(self, table_target_name, items):
        """Insert items into a DynamoDB table using batch writer.

        Args:
            table_target_name (str): Name of the target table
            items (list): List of items to insert
        """
        table_target = self.dynamodb.Table(table_target_name)
        with table_target.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=self.pre_process(item))

    def _create_table(self, partition_id, table_name):
        """Create a new DynamoDB table with specified partition key.

        Args:
            partition_id (str): Field to use as partition key
            table_name (str): Name of the table to create
        """
        logging.info("Creating table: %s", table_name)
        attribute_definitions = [{"AttributeName": partition_id, "AttributeType": "S"}]
        key_schema = [{"AttributeName": partition_id, "KeyType": "HASH"}]

        try:
            self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = self.dynamodb_client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)
        except ClientError as e:
            logging.error("Error creating table: %s", str(e))

    def _clean_all_tables(self):
        """Delete all tables in the DynamoDB region."""
        response = self.dynamodb_client.list_tables()
        tables = response.get("TableNames", [])

        if not tables:
            logging.debug("No tables found in the region.")
            return

        for table_name in tables:
            logging.info("Deleting table: %s", table_name)
            self.dynamodb_client.delete_table(TableName=table_name)

        logging.info("Waiting for tables to be deleted...")
        waiter = self.dynamodb_client.get_waiter("table_not_exists")
        for table_name in tables:
            waiter.wait(TableName=table_name)

        logging.info("All tables deleted successfully!")

    def _get_all_files_by_chain(self, chain: str, file_type=None):
        """Get all files associated with a specific chain.

        Args:
            chain (str): Chain identifier
            file_type (str, optional): Type of files to filter by

        Returns:
            list: List of files matching the criteria
        """
        table = self.dynamodb.Table("ParserStatus")
        filter_condition = Attr("index").contains(chain)
        if file_type is not None:
            filter_condition = filter_condition & Attr("index").contains(file_type)

        response = table.scan(FilterExpression=filter_condition)
        result = []
        for item in response.get("Items", []):
            result.extend(item["response"]["files_to_process"])
        return result

    def _get_content_of_file(self, table_name, file):
        """Retrieve content of a specific file from a table.

        Args:
            table_name (str): Name of the table
            file (str): File identifier

        Returns:
            list: List of items matching the file
        """
        table = self.dynamodb.Table(table_name)
        response = table.scan(FilterExpression=Attr("file_name").eq(file))
        return response.get("Items", [])

    def is_parser_updated(self, hours: int = 3) -> bool:
        """Check if the parser status was updated recently.

        Args:
            hours (int, optional): Number of hours to look back. Defaults to 3.

        Returns:
            bool: True if parser was updated within specified hours, False otherwise
        """
        try:
            table_desc = self.dynamodb_client.describe_table(TableName="ParserStatus")
            last_modified = table_desc["Table"].get("LastUpdateTime", None)

            if not last_modified:
                return False

            return (datetime.now(last_modified.tzinfo) - last_modified) < timedelta(
                hours=hours
            )

        except ClientError as e:
            logging.error(
                "Error checking DynamoDB ParserStatus update time: %s", str(e)
            )
            return False
