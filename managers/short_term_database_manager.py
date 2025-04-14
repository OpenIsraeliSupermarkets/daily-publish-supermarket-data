import os
import pandas as pd
import json
import logging
from remotes import ShortTermDatabaseUploader
from managers.cache_manager import CacheManager
from utils import now
from models import init_dynamic_tables_from_parser_status

class ShortTermDBDatasetManager:
    def __init__(
        self,
        app_folder,
        short_term_db_target:ShortTermDatabaseUploader,
        parser_table_name="ParserStatus",
        scraper_table_name="ScraperStatus",
    ):
        self.uploader = short_term_db_target()
        self.parser_table_name = parser_table_name
        self.scraper_table_name = scraper_table_name

    def _file_name_to_table(self, filename):
        return filename.split(".")[0]

    def _create_data_folders(self, outputs_folder):
        init_dynamic_tables_from_parser_status(
            f"{outputs_folder}/parser-status.json",
            self.uploader.engine
        )

    def _create_data_table(self, table_name):
        try:
            self.uploader._create_table("row_index", table_name)
        except Exception as e:
            pass

    def _create_all_tables(self, outputs_folder):
        self._create_data_folders(outputs_folder)
        self._create_status_tables()

    def push_parser_status(self, outputs_folder):
        with open(f"{outputs_folder}/parser-status.json", "r") as file:
            records = json.load(file)
        exection_time = now().strftime(
            "%d%m%Y%H%M%S"
        )

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
        with CacheManager(self.app_folder) as local_cache:
            if not local_cache:
                self.uploader._clean_all_tables()
                self._create_all_tables(outputs_folder)

            # push
            self.push_parser_status(outputs_folder)
            self.push_scraper_status_files(status_folder, local_cache)
            self.push_files_data(outputs_folder, local_cache)

        logging.info("Upload completed successfully.")
