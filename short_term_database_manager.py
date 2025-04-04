import os
import pandas as pd
import json
import re
import logging
import datetime
import pytz
from remotes import ShortTermDatabaseUploader
from cache_manager import CacheManager


class ShortTermDBDatasetManager:
    def __init__(
        self,
        app_folder,
        short_term_db_target,
        parser_table_name="ParserStatus",
        scraper_table_name="ScraperStatus",
    ):
        self.uploader: ShortTermDatabaseUploader = short_term_db_target()
        self.cache_manager = CacheManager(app_folder)
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

                    self._create_data_table(table_name)

    def get_content_of_file(self, table_name, file):
        """Retrieve content of a specific file.

        Args:
            table_name (str): Name of the table/collection
            file (str): File identifier

        Returns:
            list: List of items matching the file
        """
        return self.uploader._get_table_content(table_name, {"file_name": file})

    def is_parser_updated(self, seconds: int = 10800) -> bool:
        """Check if the parser was updated recently.

        Args:
            seconds (int, optional): Time window in seconds to check for updates.
                                   Defaults to 10800 (3 hours).

        Returns:
            bool: True if parser was updated within specified time window, False otherwise
        """
        return self.uploader._is_collection_updated("ParserStatus", seconds)

    def get_all_files_by_chain(self, chain: str, file_type=None):
        """Get all files associated with a specific chain.

        Args:
            chain (str): Chain identifier
            file_type (str, optional): Type of files to filter by

        Returns:
            list: List of files matching the criteria
        """
        filter_condition = f".*{re.escape(chain)}.*"
        if file_type is not None:
            filter_condition = f".*{re.escape(file_type)}.*{re.escape(chain)}.*"

        docs = self.uploader._get_table_content(
            "ParserStatus", {"index": {"$regex": filter_condition}}
        )
        files = []
        for doc in docs:
            if "response" in doc and "files_to_process" in doc["response"]:
                files.extend(doc["response"]["files_to_process"])
        return files

    def _create_data_table(self, table_name):
        try:
            self.uploader._create_table("row_index", table_name)
        except Exception as e:
            pass

    def _create_all_tables(self, outputs_folder):
        self._create_data_folders(outputs_folder)
        self._create_status_tables()

    def _create_status_tables(self):
        self.uploader._create_table(
            "index",
            self.parser_table_name,
        )
        self.uploader._create_table(
            "index",
            self.scraper_table_name,
        )

    def _now(self):
        return datetime.datetime.now(pytz.timezone("Asia/Jerusalem")).strftime(
            "%d%m%Y%H%M%S"
        )

    def push_parser_status(self, outputs_folder):
        with open(f"{outputs_folder}/parser-status.json", "r") as file:
            records = json.load(file)
        exection_time = self._now()

        records = [
            {
                "index": record["file_type"]
                + "@"
                + record["store_enum"]
                + "@"
                + exection_time,
                "ChainName": record["store_enum"],
                "timestamp": exection_time,
                **record,
            }
            for record in records
        ]
        self.uploader._insert_to_database(self.parser_table_name, records)
        logging.info("Parser status stored in DynamoDB successfully.")

    def push_scraper_status_files(self, status_folder, local_cahce):
        records = []
        for file in os.listdir(status_folder):
            if file.endswith(".json") and file != "parser-status.json":
                with open(os.path.join(status_folder, file), "r") as f:
                    data = json.load(f)

                pushed_timestamp = local_cahce.get(file, {}).get("timestamps", [])

                for index, (timestamp, actions) in enumerate(data.items()):
                    logging.info(f"Pushing {file}: {timestamp} vs {pushed_timestamp}")

                    if timestamp == "verified_downloads":
                        continue

                    if timestamp in pushed_timestamp:
                        continue

                    for action in actions:
                        records.append(
                            {
                                "index": file.split(".")[0]
                                + "@"
                                + action["status"]
                                + "@"
                                + timestamp
                                + "@"
                                + str(index),
                                "file_name": file.split(".")[0],
                                "timestamp": timestamp,
                                **action,
                            }
                        )
                    pushed_timestamp.append(timestamp)

                if file not in local_cahce:
                    local_cahce[file] = {}
                local_cahce[file]["timestamps"] = pushed_timestamp

        if records:
            self.uploader._insert_to_database(self.scraper_table_name, records)

    def push_files_data(self, outputs_folder, local_cahce):
        #
        for file in os.listdir(outputs_folder):

            if not file.endswith(".csv"):
                continue
            # the path to process
            file_path = os.path.join(outputs_folder, file)
            
            logging.info(f"Pushing {file}")
            # select the correct table
            table_target_name = self._file_name_to_table(file)
            self._create_data_table(table_target_name)

            # Read the CSV file into a DataFrame
            last_row = local_cahce.get("last_pushed", {}).get(file, -1)
            logging.info(f"Last row: {last_row}")
            # Process the CSV file in chunks to reduce memory usage
            chunk_size = 10000
            previous_row = None
            header = pd.read_csv(file_path, nrows=0)

            for chunk in pd.read_csv(
                file_path,
                skiprows=lambda x: x < last_row + 1,
                names=header.columns,
                chunksize=chunk_size,
            ):

                if not chunk.empty:
                    chunk.index = range(last_row + 1, last_row + 1 + len(chunk))
                    logging.info(
                        f"Batch start: {chunk.iloc[0].name}, end: {chunk.iloc[-1].name}"
                    )

                    if previous_row is not None:
                        chunk = pd.concat([previous_row, chunk])

                    chunk = chunk.reset_index(names=["row_index"])
                    last_row = max(last_row, int(chunk.row_index.max()))
                    chunk["row_index"] = chunk["row_index"].astype(str)
                    items = chunk.ffill().to_dict(orient="records")
                    self.uploader._insert_to_database(table_target_name, items[1:])

                    # Save last row for next iteration
                    previous_row = chunk.drop(columns=["row_index"]).tail(1)

            if "last_pushed" not in local_cahce:
                local_cahce["last_pushed"] = {}
            local_cahce["last_pushed"][file] = last_row

            logging.info(f"Completed pushing {file}")

        logging.info("Files data pushed in DynamoDB successfully.")

    def upload(self, outputs_folder, status_folder):
        local_cache = self.cache_manager.load()
        if not local_cache:
            self.uploader._clean_all_tables()
            self._create_all_tables(outputs_folder)

        # push
        self.push_parser_status(outputs_folder)
        self.push_scraper_status_files(status_folder, local_cache)
        self.push_files_data(outputs_folder, local_cache)
        self.cache_manager.save(local_cache)

        logging.info("Upload completed successfully.")
