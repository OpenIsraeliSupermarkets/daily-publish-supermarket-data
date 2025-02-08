import os
import pandas as pd
import json
import logging
import datetime
import pytz
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
                
                for timestamp, actions in data.items():
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
                                + timestamp,
                                "file_name": file.split(".")[0],
                                "timestamp": timestamp,
                                **action,
                            }
                        )
                    pushed_timestamp.append(timestamp)
            
            local_cahce[file] = {
                "timestamps":pushed_timestamp
            } 

        self.uploader._insert_to_database(self.scraper_table_name, records)
        logging.info("Scraper status files stored in DynamoDB successfully.")

    def push_files_data(self, outputs_folder, local_cahce):
        #
        for file in os.listdir(outputs_folder):

            if not file.endswith(".csv"):
                continue

            logging.info(f"Pushing {file}")
            # select the correct table
            table_target_name = self._file_name_to_table(file)

            # Read the CSV file into a DataFrame
            last_row = local_cahce.get("last_pushed",{}).get(file, -1)
            # Process the CSV file in chunks to reduce memory usage
            chunk_size = 10000
            previous_row = None
            for chunk in pd.read_csv(os.path.join(outputs_folder, file), 
                                   skiprows=range(1, last_row + 2) if last_row > -1 else None,
                                   chunksize=chunk_size):
                
                if not chunk.empty:
                    if previous_row is not None:
                        chunk = pd.concat([previous_row, chunk])
                    
                    chunk = chunk.reset_index(names=["row_index"])
                    last_row = max(last_row,int(chunk.row_index.max()))
                    chunk["row_index"] = chunk["row_index"].astype(str)
                    items = chunk.ffill().to_dict(orient="records")
                    self.uploader._insert_to_database(table_target_name, items[1:])
                    
                    # Save last row for next iteration
                    previous_row = chunk.drop(columns=['row_index']).tail(1)

            local_cahce['last_pushed'] = {file: last_row}

            logging.info(f"Completed pushing {file}")

        logging.info("Files data pushed in DynamoDB successfully.")

    def _load_cache(self):
        last_pushed = {}

        if os.path.exists(self.cache_file):
            with open(self.cache_file, "r") as file:
                last_pushed = json.load(file)
        return last_pushed

    def _upload_local_cache(self, new_content):
        with open(self.cache_file, "w") as file:
            json.dump(new_content, file)

    def upload(self, outputs_folder, status_folder):
        local_cahce = self._load_cache()
        if not local_cahce:
            self.uploader._clean_all_tables()
            self._create_all_tables(outputs_folder)

        # push
        self.push_parser_status(outputs_folder)
        self.push_scraper_status_files(status_folder,local_cahce)
        self.push_files_data(outputs_folder, local_cahce)
        self._upload_local_cache(local_cahce)
