import os
import csv
import json
import datetime
import pytz
import logging
import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError


class DynamoDBDatasetManager:
    def __init__(
        self,
        parser_table_name="ParserStatus",
        scraper_table_name="ScraperStatus",
        region_name="us-east-1",
    ):
        self.dynamodb = boto3.resource("dynamodb", region_name=region_name)
        self.dynamodb_client = boto3.client("dynamodb", region_name=region_name)

        self.parser_table_name = parser_table_name
        self.scraper_table_name = scraper_table_name

    def _now(self):
        return datetime.datetime.now(pytz.timezone("Asia/Jerusalem")).strftime(
            "%d/%m/%Y, %H:%M:%S"
        )

    def _create_all_tables(self, outputs_folder):
        with open(f"{outputs_folder}/parser-status.json", "r") as file:
            data = json.load(file)

        file_targets = {}
        for entry in data:

            if "response" in entry and entry["response"]["file_was_created"]:
                filename = os.path.basename(entry["response"]["file_created_path"])
                table_name = entry["response"]["files_types"] + entry["store_enum"]

                self._create_tables("row_index", [("row_index", str)], table_name)

                file_targets[filename] = table_name
        return file_targets

    def get_dynamodb_type(self, py_type):
        """
        Maps a Python type to a DynamoDB type string.

        :param py_type: Python type (e.g., str, int, list)
        :return: Corresponding DynamoDB type string
        """
        type_mapping = {
            str: "S",  # String
            int: "N",  # Number
            float: "N",  # Number
            bool: "BOOL",  # Boolean
            type(None): "NULL",  # Null
            list: "L",  # List
            dict: "M",  # Map
            set: "SS",  # Default to StringSet (caller must ensure correct type)
            bytes: "B",  # Binary
            bytearray: "B",  # Binary
        }

        return type_mapping.get(
            py_type, "UNKNOWN"
        )  # Default to UNKNOWN if type is not mapped

    def _create_tables(self, partition_id, partition_keys, table_name):
        # Define attribute definitions
        attribute_definitions = [
            {"AttributeName": partition_id, "AttributeType": "S"}
        ]  # Adjust data type if needed

        # Define key schema
        key_schema = [
            {"AttributeName": partition_id, "KeyType": "HASH"}
        ]  # Partition key

        # Create DynamoDB table
        try:
            self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
            waiter = self.dynamodb_client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)
        except Exception as e:
            logging.error(f"Error: {e}")

    def _clean_all_tables(self):
        # List all tables
        response = self.dynamodb_client.list_tables()
        tables = response.get("TableNames", [])

        if not tables:
            logging.debug("No tables found in the region.")
        else:
            for table in tables:
                logging.info(f"Deleting table: {table}")
                self.dynamodb_client.delete_table(TableName=table)

            logging.info("Waiting for tables to be deleted...")

            # Wait for all tables to be deleted
            waiter = self.dynamodb_client.get_waiter("table_not_exists")
            for table in tables:
                waiter.wait(TableName=table)

            logging.info("All tables deleted successfully!")

    def push_parser_status(self, outputs_folder):
        with open(f"{outputs_folder}/parser-status.json", "r") as file:
            data = json.load(file)

        try:
            self._create_tables(
                "file_name",
                [("file_name", str), ("content", list)],
                self.parser_table_name,
            )
            parser_table = self.dynamodb.Table(self.parser_table_name)
            parser_table.put_item(
                Item={"file_name": os.path.basename(file.name), "content": data}
            )
            logging.info("Parser status stored in DynamoDB successfully.")
        except (BotoCoreError, NoCredentialsError) as e:
            logging.error(f"Error writing to DynamoDB: {e}")
            raise

    def push_scraper_status_files(self, status_folder):
        try:
            self._create_tables(
                "file_name",
                [("file_name", str), ("content", list)],
                self.scraper_table_name,
            )
            scraper_table = self.dynamodb.Table(self.scraper_table_name)
            for file in os.listdir(status_folder):
                if file.endswith(".json"):
                    with open(os.path.join(status_folder, file), "r") as f:
                        data = json.load(f)
                        # Convert float types to Decimal types
                        from decimal import Decimal

                        def convert_floats(item):
                            if isinstance(item, list):
                                return [convert_floats(i) for i in item]
                            elif isinstance(item, dict):
                                return {k: convert_floats(v) for k, v in item.items()}
                            elif isinstance(item, float):
                                return Decimal(str(item))
                            return item

                        data = convert_floats(data)
                        scraper_table.put_item(
                            Item={"file_name": file, "content": data}
                        )
            logging.info("Scraper status files stored in DynamoDB successfully.")
        except (BotoCoreError, NoCredentialsError) as e:
            logging.error(f"Error writing to DynamoDB: {e}")
            raise

    def push_files_data(self, outputs_folder, file_targets):
        #
        for file in os.listdir(outputs_folder):
            table_target_name = file_targets[file]
            table_target = self.dynamodb.Table(table_target_name)

            with open(os.path.join(outputs_folder, file), "r") as csv_file:
                reader = csv.DictReader(
                    csv_file
                )  # Use DictReader to read rows as dictionaries
                for row in reader:
                    # Prepare the item to insert into DynamoDB
                    item = {key: value for key, value in row.items()}

                    # Insert the item into DynamoDB
                    table_target.put_item(Item=item)
            logging.info("Files metadata stored in DynamoDB successfully.")

    def compose(self, outputs_folder, status_folder):
        self._clean_all_tables()
        file_targets = self._create_all_tables(outputs_folder)
        self.push_parser_status(outputs_folder)
        self.push_scraper_status_files(status_folder)
        self.push_files_data(outputs_folder, file_targets)
