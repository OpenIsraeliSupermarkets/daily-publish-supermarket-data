import os
import pandas as pd
import json
import logging
from remotes import ShortTermDatabaseUploader
from managers.cache_manager import CacheManager, CacheState
from managers.large_file_push_manager import LargeFilePushManager
from data_models.raw_schema import ParserStatus, ScraperStatus
from datetime import datetime


class ShortTermDBDatasetManager:
    def __init__(
        self,
        app_folder,
        outputs_folder,
        status_folder,
        short_term_db_target: ShortTermDatabaseUploader,
    ):
        self.app_folder = app_folder
        self.uploader = short_term_db_target
        self.outputs_folder = outputs_folder
        self.status_folder = status_folder

    def _push_parser_status(self, local_cahce: CacheState):
        with open(f"{self.outputs_folder}/parser-status.json", "r") as file:
            records = json.load(file)

        pushed_timestamps = local_cahce.get_pushed_timestamps("parser-status.json")
        added_timestamps = []
        processed_records = []

        for record in records:
            if record["when_date"] not in pushed_timestamps:
                processed_records.append(
                    ParserStatus(
                        index=ParserStatus.to_index(
                            record["file_type"],
                            record["store_enum"],
                            record["when_date"],
                        ),
                        when_date=record["when_date"],
                        requested_limit=record["limit"],
                        requested_store_enum=record["store_enum"],
                        requested_file_type=record["file_type"],
                        scaned_data_folder=record["data_folder"],
                        output_folder=record["output_folder"],
                        status=record["status"],
                        response=record["response"],
                    ).to_dict()
                )
                added_timestamps.append(record["when_date"])

        self.uploader._insert_to_database(
            ParserStatus.get_table_name(), processed_records
        )

        local_cahce.update_pushed_timestamps(
            "parser-status.json", list(set(added_timestamps)) + pushed_timestamps
        )

        logging.info("Parser status stored in DynamoDB successfully.")

    def _push_status_files(self, local_cahce: CacheState):
        for file in os.listdir(self.status_folder):
            if not file.endswith(".json"):
                logging.warn(f"Skipping '{file}', should we store it?")
                continue

            self._push_scraper_status(file, local_cahce)

        self._push_parser_status(local_cahce)

    def _push_scraper_status(self, file_name: str, local_cahce: CacheState):

        with open(os.path.join(self.status_folder, file_name), "r") as f:
            data = json.load(f)

        pushed_timestamp = local_cahce.get_pushed_timestamps(file_name)
        logging.info(f"Pushing {file_name}: already pushed {pushed_timestamp}")

        records = []
        for index, (timestamp, actions) in enumerate(data.items()):

            if timestamp == "verified_downloads":
                continue

            if timestamp in pushed_timestamp:
                continue

            logging.info(f"Pushing {file_name}: {timestamp}")
            for action in actions:
                records.append(
                    ScraperStatus(
                        index=ScraperStatus.to_index(
                            file_name.split(".")[0],
                            action["status"],
                            timestamp,
                            str(index),
                        ),
                        file_name=file_name.split(".")[0],
                        timestamp=datetime.strptime(timestamp, "%Y%m%d%H%M%S").strftime(
                            "%Y-%m-%d %H:%M:%S.%f%z"
                        ),
                        status=action["status"],
                        when=action["when"],
                        status_data={
                            key: value
                            for key, value in action.items()
                            if key != "status" and key != "when"
                        },
                    ).to_dict()
                )

            pushed_timestamp.append(timestamp)

        local_cahce.update_pushed_timestamps(file_name, pushed_timestamp)

        self.uploader._insert_to_database(ScraperStatus.get_table_name(), records)

    def _push_files_data(self, local_cahce: CacheState):
        #
        for file in os.listdir(self.outputs_folder):
            if not file.endswith(".csv"):
                logging.warn(f"Skipping '{file}', should we store it?")
                continue

            large_file_pusher = LargeFilePushManager(self.outputs_folder, self.uploader)
            large_file_pusher.process_file(file, local_cahce)

        logging.info("Files data pushed in DynamoDB successfully.")

    def upload(self, force_restart=False):
        """
        Upload the data to the database.
        """
        with CacheManager(self.app_folder) as local_cache:
            if local_cache.is_empty() or force_restart:
                self.uploader.restart_database()

            # push
            self._push_status_files(local_cache)
            self._push_files_data(local_cache)

        logging.info("Upload completed successfully.")
