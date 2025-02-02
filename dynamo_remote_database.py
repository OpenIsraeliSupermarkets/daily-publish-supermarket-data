import os
import pandas as pd
import json
import datetime
import pytz
import logging
import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError
from decimal import Decimal


class DynamoDBDatasetManager:
    def __init__(
        self,
        app_folder,
        parser_table_name="ParserStatus",
        scraper_table_name="ScraperStatus",
        region_name="us-east-1",
    ):
        self.dynamodb = boto3.resource("dynamodb", region_name=region_name)
        self.dynamodb_client = boto3.client("dynamodb", region_name=region_name)

        self.cache_file = os.path.join(app_folder,".push_cache")
        self.parser_table_name = parser_table_name
        self.scraper_table_name = scraper_table_name

    def _now(self):
        return datetime.datetime.now(pytz.timezone("Asia/Jerusalem")).strftime(
            "%d/%m/%Y, %H:%M:%S"
        )

    def _file_name_to_table(self, filename):
        return filename.split(".")[0]

    def _create_data_folders(self, outputs_folder):
        with open(f"{outputs_folder}/parser-status.json", "r") as file:
            data = json.load(file)

        for entry in data:
            if "response" in entry and entry["response"]["file_was_created"]:

                data_file_path = entry["response"]["file_created_path"]
                if os.path.exists(data_file_path):
                    filename = os.path.basename(entry["response"]["file_created_path"])
                    table_name = self._file_name_to_table(filename)

                    self._create_table("row_index", table_name)

    def _create_all_tables(self, outputs_folder):
        self._create_data_folders(outputs_folder)
        self._create_status_tables()

    def _insert_to_database(self, table_target, items):
        # Batch write items to DynamoDB
        with table_target.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=self.pre_process(item))

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

    def _create_table(self, partition_id, table_name):
        logging.info(f"creating {table_name}")
        # Define attribute definitions
        attribute_definitions = [{"AttributeName": partition_id, "AttributeType": "S"}]

        # Define key schema
        key_schema = [{"AttributeName": partition_id, "KeyType": "HASH"}]
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

    def _create_status_tables(self):
        self._create_table(
            "file_name",
            self.parser_table_name,
        )
        self._create_table(
            "file_name",
            self.scraper_table_name,
        )

    def pre_process(self, item):
        if isinstance(item, list):
            return [self.pre_process(i) for i in item]
        elif isinstance(item, dict):
            return {k: self.pre_process(v) for k, v in item.items()}
        elif isinstance(item, float):
            return Decimal(str(item))
        return item

    def push_parser_status(self, outputs_folder):
        with open(f"{outputs_folder}/parser-status.json", "r") as file:
            data = json.load(file)
        try:
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
            scraper_table = self.dynamodb.Table(self.scraper_table_name)
            for file in os.listdir(status_folder):
                if file.endswith(".json"):
                    with open(os.path.join(status_folder, file), "r") as f:
                        data = json.load(f)
                        # Convert float types to Decimal types
                        scraper_table.put_item(
                            Item={"file_name": file, "content": self.pre_process(data)}
                        )
            logging.info("Scraper status files stored in DynamoDB successfully.")
        except (BotoCoreError, NoCredentialsError) as e:
            logging.error(f"Error writing to DynamoDB: {e}")
            raise

    def push_files_data(self, outputs_folder):
        #
        for file in os.listdir(outputs_folder):

            if file == "parser-status.json":
                continue

            logging.info(f"Pushing {file}")

            # select the correct table
            table_target_name = self._file_name_to_table(file)
            table_target = self.dynamodb.Table(table_target_name)

            # Read the CSV file into a DataFrame
            df = pd.read_csv(os.path.join(outputs_folder, file))
            df = df.reset_index(names=["row_index"])    
            df = df[df.row_index > last_pushed.get(file,-1)]
            latast = df.row_index.max()
            df["row_index"] = df["row_index"].astype(str)
            items = df.ffill().to_dict(orient="records")
            self._insert_to_database(table_target, items)
            
            last_pushed = {file:latast}

            logging.info(f"Completed pushing {file}")

        self._upload_local_cache(last_pushed)
        logging.info("Files data pushed in DynamoDB successfully.")

    def _load_cache(self):
        last_pushed = {}
        
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r') as file:
                last_pushed = json.load(file)
        return last_pushed
    
    def _upload_local_cache(self,**new_content):
        with open(self.cache_file, 'w') as file:
            json.dump(new_content,file)
    
    def upload(self, app_folder, outputs_folder):
        local_cahce = self._load_cache()
        if not local_cahce:
            self._clean_all_tables()
            self._create_all_tables(outputs_folder)
        
        # push
        self.push_parser_status(outputs_folder)
        self.push_scraper_status_files(outputs_folder)
        self.push_files_data(app_folder, outputs_folder)
