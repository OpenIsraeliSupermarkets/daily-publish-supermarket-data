from abc import ABC, abstractmethod
from kaggle import KaggleApi
import os
import logging
import json
import shutil
import boto3
from decimal import Decimal


class RemoteDatabaseUploader(ABC):
    """
    Abstract class for uploading data to a remote database.
    """

    @abstractmethod
    def increase_index(self):
        """
        Define the new index.
        """
        pass

    @abstractmethod
    def upload_to_dataset(self, message):
        """
        Upload a dataset to the remote.
        """
        pass

    @abstractmethod
    def clean(self):
        """
        Clean the dataset.
        """
        pass


class Dummy(RemoteDatabaseUploader):
    """
    Uploads data to a remote database.
    """

    def __init__(self, dataset_remote_name, dataset_path, when):
        self.dataset_remote_name = dataset_remote_name
        self.dataset_path = dataset_path
        self.when = when

    def increase_index(self):
        """
        Increase the index.
        """
        pass

    def upload_to_dataset(self, message):
        """
        Upload the dataset.
        """
        logging.info(
            f"Uploading dataset '{self.dataset_remote_name}' to remote database, message {message}"
        )
        server_path = f"remote_{self.dataset_remote_name}"
        os.makedirs(server_path, exist_ok=True)
        for filename in os.listdir(self.dataset_path):
            file_path = os.path.join(self.dataset_path, filename)
            if os.path.isfile(file_path):
                shutil.copy(file_path, server_path)

    def clean(self):
        pass


class KaggleUploader(RemoteDatabaseUploader):

    def __init__(self, dataset_remote_name, dataset_path, when):
        self.dataset_remote_name = dataset_remote_name
        self.dataset_path = dataset_path
        self.when = when
        self.api = KaggleApi()
        self.api.authenticate()

    def increase_index(self):
        """
        Download a dataset from Kaggle.

        :param dataset: str, the dataset to download in the format 'owner/dataset-name'
        :param path: str, the path where to save the dataset (default is current directory)
        """

        self.api.dataset_download_cli(
            f"erlichsefi/{self.dataset_remote_name}", file_name="index.json", force=True
        )
        print(f"Dataset '{self.dataset_remote_name}' downloaded successfully")

        with open("index.json", "r") as file:
            index = json.load(file)

        index[max(map(int, index.keys())) + 1] = self.when

        with open(os.path.join(self.dataset_path, "index.json"), "w+") as file:
            json.dump(index, file)

    def upload_to_dataset(self, message):
        self.api.dataset_create_version(
            folder=self.dataset_path,
            version_notes=message,
            delete_old_versions=False,
        )

    def clean(self):
        os.remove("index.json")


class APIDatabaseUploader:

    def __init__(self):
        pass

    def _insert_to_database(self, table_target_name, items):
        pass

    def _create_table(self, partition_id, table_name):
        pass

    def _clean_all_tables(self):
        pass


class DynamoDbUploader(APIDatabaseUploader):

    def __init__(self, region_name):
        self.dynamodb = boto3.resource("dynamodb", region_name=region_name)
        self.dynamodb_client = boto3.client("dynamodb", region_name=region_name)

    def pre_process(self, item):
        if isinstance(item, list):
            return [self.pre_process(i) for i in item]
        elif isinstance(item, dict):
            return {k: self.pre_process(v) for k, v in item.items()}
        elif isinstance(item, float):
            return Decimal(str(item))
        return item

    def _insert_to_database(self, table_target_name, items):
        # Batch write items to DynamoDB
        table_target = self.dynamodb.Table(table_target_name)
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


class DummyDocumentDbUploader:
    def __init__(self, db_path="./document_db"):
        self.db_path = db_path
        os.makedirs(self.db_path, exist_ok=True)

    def _insert_to_database(self, table_target_name, items):
        table_path = os.path.join(self.db_path, table_target_name)
        os.makedirs(table_path, exist_ok=True)

        for item in items:
            item_id = item.get("id", None)
            if not item_id:
                logging.error("Item must have an 'id' field.")
                continue

            file_path = os.path.join(table_path, f"{item_id}.json")
            with open(file_path, "w") as f:
                json.dump(item, f, indent=4)

    def _create_table(self, partition_id, table_name):
        table_path = os.path.join(self.db_path, table_name)
        os.makedirs(table_path, exist_ok=True)
        logging.info(f"Created table: {table_name}")

    def _clean_all_tables(self):
        for table_name in os.listdir(self.db_path):
            table_path = os.path.join(self.db_path, table_name)
            if os.path.isdir(table_path):
                for file in os.listdir(table_path):
                    os.remove(os.path.join(table_path, file))
                os.rmdir(table_path)
        logging.info("All tables deleted successfully!")
