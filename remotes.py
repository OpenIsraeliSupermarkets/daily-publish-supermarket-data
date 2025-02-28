from abc import ABC, abstractmethod
import os
import logging
import json
import shutil
import boto3
import re
from decimal import Decimal
from boto3.dynamodb.conditions import Attr, Key
import pymongo


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


class DummyFileStorge(RemoteDatabaseUploader):
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
        from kaggle import KaggleApi

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

    def __init__(self, *_):
        pass

    def _insert_to_database(self, table_target_name, items):
        pass

    def _create_table(self, partition_id, table_name):
        pass

    def _clean_all_tables(self):
        pass

    def _get_all_files_by_chain(self, chain: str, file_type=None):
        pass

    def _get_content_of_file(self, table_name, file):
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

    def _get_all_files_by_chain(self, chain: str, file_type=None):
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
        table = self.dynamodb.Table(table_name)
        response = table.scan(FilterExpression=Attr("file_name").eq(file))
        return response.get("Items", [])


class DummyDocumentDbUploader:
    def __init__(self, db_path):
        self.db_path = os.path.join("./document_db", db_path)
        os.makedirs(self.db_path, exist_ok=True)
        self._load_tables_ids()

    def _load_tables_ids(self):
        tables_ids_path = os.path.join(self.db_path, "tables_ids.json")
        if os.path.exists(tables_ids_path):
            with open(tables_ids_path, "r") as f:
                self.tables_ids = json.load(f)
        else:
            self.tables_ids = {}

    def _save_tables_ids(self):
        tables_ids_path = os.path.join(self.db_path, "tables_ids.json")
        with open(tables_ids_path, "w") as f:
            json.dump(self.tables_ids, f, indent=4, ensure_ascii=False)

    def _clean_meta_data(self):
        if os.path.exists(os.path.join(self.db_path, "tables_ids.json")):
            os.remove(os.path.join(self.db_path, "tables_ids.json"))

    def _insert_to_database(self, table_target_name, items):
        table_path = os.path.join(self.db_path, table_target_name)
        os.makedirs(table_path, exist_ok=True)

        id_name = self.tables_ids[table_target_name]
        for item in items:
            item_id = item.get(id_name, None)
            if not item_id:
                logging.error(f"Item must have an '{item_id}' field.")
                continue

            file_path = os.path.join(table_path, f"{item_id}.json")
            with open(file_path, "w") as f:
                json.dump(item, f, indent=4, ensure_ascii=False)

    def _create_table(self, partition_id, table_name):
        table_path = os.path.join(self.db_path, table_name)
        os.makedirs(table_path, exist_ok=True)
        self.tables_ids[table_name] = partition_id
        self._save_tables_ids()
        logging.info(f"Created table: {table_name}")

    def _clean_all_tables(self):
        self._clean_meta_data()
        for table_name in os.listdir(self.db_path):
            table_path = os.path.join(self.db_path, table_name)
            if os.path.isdir(table_path):
                for file in os.listdir(table_path):
                    os.remove(os.path.join(table_path, file))
                os.rmdir(table_path)
        logging.info("All tables deleted successfully!")

    def _get_all_files_by_chain(self, chain: str, file_type: str = None):
        chain_path = os.path.join(self.db_path, "ParserStatus")
        if not os.path.exists(chain_path):
            return []

        file_found = []
        for file in os.listdir(chain_path):
            if chain in file and (file_type is None or file_type in file):
                if os.path.isfile(os.path.join(chain_path, file)):
                    raise Exception(f"File {file} is not a file")
                with open(os.path.join(chain_path, file), "r") as file:
                    file_found.extend(json.load(file)["response"]["files_to_process"])
        return file_found

    def _get_content_of_file(self, table_name, content_of_file):
        folder_path = os.path.join(self.db_path, table_name)
        if not os.path.exists(folder_path):
            logging.error(f"File '{file}' does not exist in table '{table_name}'.")
            return None

        file_found = []
        for file in os.listdir(folder_path):
            with open(os.path.join(folder_path, file), "r") as file:
                data = json.load(file)
                if data["file_name"] == content_of_file:
                    file_found.append(data)
        return file_found


class MongoDbUploader(APIDatabaseUploader):

    def __init__(self, mongodb_uri):
        self.client = pymongo.MongoClient(
            os.getenv("MONGODB_URI", "mongodb://host.docker.internal:27017")
        )
        self.db = self.client.supermarket_data

    def pre_process(self, item):
        """Convert large integers to strings to avoid MongoDB limitations"""
        if isinstance(item, list):
            return [self.pre_process(i) for i in item]
        elif isinstance(item, dict):
            return {k: self.pre_process(v) for k, v in item.items()}
        elif isinstance(item, int) and (item > 2**63 - 1 or item < -(2**63)):
            return str(item)
        return item

    def _insert_to_database(self, table_target_name, items):
        logging.info(f"Pushing to table {table_target_name}, {len(items)} items")
        collection = self.db[table_target_name]
        if items:
            processed_items = map(self.pre_process, items)
            try:
                # ניסיון להכניס את כל הרשומות בבת אחת
                collection.insert_many(processed_items, ordered=False)
                logging.info(
                    f"Successfully inserted {len(processed_items)} records to DynamoDB"
                )
            except Exception as e:
                # אם נכשל, ננסה להכניס כל רשומה בנפרד
                logging.warning(f"Bulk insert failed, trying individual inserts.")
                successful_records = 0
                for record in processed_items:
                    try:
                        collection.insert_one(record)
                        successful_records += 1
                    except Exception as inner_e:
                        pass

    def _create_table(self, partition_id, table_name):
        logging.info(f"Creating collection: {table_name}")
        try:
            self.db.create_collection(table_name)
            self.db[table_name].create_index(
                [(partition_id, pymongo.ASCENDING)], unique=True
            )
        except Exception as e:
            logging.error(f"Error creating collection: {e}")

    def _clean_all_tables(self):
        for collection in self.db.list_collection_names():
            self.db[collection].drop()
        logging.info("All collections deleted successfully!")

    def _get_all_files_by_chain(self, chain: str, file_type=None):
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
        collection = self.db[table_name]
        results = []
        for obj in collection.find({"file_name": file}):
            # Convert ObjectId to dict manually
            obj_dict = {k: v for k, v in obj.items() if k != "_id"}
            results.append(obj_dict)
        return results
