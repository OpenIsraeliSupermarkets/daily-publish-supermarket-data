import os
import pandas as pd
import json
import datetime
import pytz
import logging

from botocore.exceptions import BotoCoreError, NoCredentialsError
from remotes import DynamoDbUploader


class ShortTermDBDatasetManager:
    def __init__(
        self,
        app_folder,
        short_term_db_target=DynamoDbUploader,
        parser_table_name="ParserStatus",
        scraper_table_name="ScraperStatus",
        region_name="us-east-1",
    ):
        self.uploader = short_term_db_target(region_name)
        self.cache_file = os.path.join(app_folder, ".push_cache")
        self.parser_table_name = parser_table_name
        self.scraper_table_name = scraper_table_name

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

                    self.uploader._create_table("row_index", table_name)

    def _create_all_tables(self, outputs_folder):
        self._create_data_folders(outputs_folder)
        self._create_status_tables()

    def _create_status_tables(self):
        self.uploader._create_table(
            "file_name",
            self.parser_table_name,
        )
        self.uploader._create_table(
            "file_name",
            self.scraper_table_name,
        )

    def push_parser_status(self, outputs_folder):
        with open(f"{outputs_folder}/parser-status.json", "r") as file:
            data = json.load(file)

        records = [{"file_name": os.path.basename(file.name), "content": data}]
        self.uploader._insert_to_database(self.parser_table_name, records)
        logging.info("Parser status stored in DynamoDB successfully.")

    def push_scraper_status_files(self, status_folder):
        records = []
        for file in os.listdir(status_folder):
            if file.endswith(".json"):
                with open(os.path.join(status_folder, file), "r") as f:
                    records.append({"file_name": file, "content": json.load(f)})

        self.uploader._insert_to_database(self.scraper_table_name, records)
        logging.info("Scraper status files stored in DynamoDB successfully.")

    def push_files_data(self, outputs_folder):
        #
        for file in os.listdir(outputs_folder):

            if file == "parser-status.json":
                continue

            logging.info(f"Pushing {file}")
            # select the correct table
            table_target_name = self._file_name_to_table(file)

            # Read the CSV file into a DataFrame
            df = pd.read_csv(os.path.join(outputs_folder, file))
            df = df.reset_index(names=["row_index"])
            df = df[df.row_index > last_pushed.get(file, -1)]
            latast = df.row_index.max()
            df["row_index"] = df["row_index"].astype(str)
            items = df.ffill().to_dict(orient="records")
            self.uploader._insert_to_database(table_target_name, items)

            last_pushed = {file: latast}

            logging.info(f"Completed pushing {file}")

        self._upload_local_cache(last_pushed)
        logging.info("Files data pushed in DynamoDB successfully.")

    def _load_cache(self):
        last_pushed = {}

        if os.path.exists(self.cache_file):
            with open(self.cache_file, "r") as file:
                last_pushed = json.load(file)
        return last_pushed

    def _upload_local_cache(self, **new_content):
        with open(self.cache_file, "w") as file:
            json.dump(new_content, file)

    def upload(self, app_folder, outputs_folder):
        local_cahce = self._load_cache()
        if not local_cahce:
            self.uploader._clean_all_tables()
            self._create_all_tables(outputs_folder)

        # push
        self.push_parser_status(outputs_folder)
        self.push_scraper_status_files(outputs_folder)
        self.push_files_data(app_folder, outputs_folder)
