import os
import pandas as pd
import json
import logging
from remotes import ShortTermDatabaseUploader
from managers.cache_manager import CacheManager, CacheState
from managers.large_file_push_manager import LargeFilePushManager
from data_models.raw import ParserStatus, ScraperStatus

class ShortTermDBDatasetManager:
    def __init__(
        self,
        app_folder,
        outputs_folder,
        status_folder,
        short_term_db_target:ShortTermDatabaseUploader
    ):
        self.app_folder = app_folder
        self.uploader = short_term_db_target()
        self.outputs_folder = outputs_folder
        self.status_folder = status_folder
    
    def _push_parser_status(self):
        with open(f"{self.outputs_folder}/parser-status.json", "r") as file:
            records = json.load(file)
        
        records = [
            ParserStatus(
                index=record["file_type"]
                + "@"
                + record["store_enum"],
                chain_name=record["store_enum"],
                requested_limit=record["limit"],
                requested_store_enum=record["store_enum"],
                requested_file_type=record["file_type"],
                scaned_data_folder=record["data_folder"],
                output_folder=record["output_folder"],
                status=record["status"]
            ).to_dict()
            for record in records
        ]
        self.uploader._insert_to_database(ParserStatus.get_table_name(), records)
        logging.info("Parser status stored in DynamoDB successfully.")

    def _push_status_files(self, local_cahce:CacheState):
        for file in os.listdir(self.status_folder):
            if not file.endswith(".json"):
                logging.warn(f"Skipping '{file}', should we store it?")
                continue
            
            if file == "parser-status.json":
                self._push_parser_status()
            else:
                self._push_scraper_status(local_cahce)

               
    def _push_scraper_status(self, file_name:str, local_cahce:CacheState):
        
        with open(os.path.join(self.status_folder, file_name), "r") as f:
            data = json.load(f)

        pushed_timestamp = local_cahce.get_pushed_timestamps(file_name)
        logging.info(f"Pushing {file_name}: {timestamp} vs {pushed_timestamp}")

        records = []
        for index, (timestamp, actions) in enumerate(data.items()):
            
            if timestamp == "verified_downloads":
                continue

            if timestamp in pushed_timestamp:
                continue

            for action in actions:
                records.append(
                    ScraperStatus(
                        index=file_name.split(".")[0]
                        + "@"
                        + action["status"]
                        + "@"
                        + timestamp
                        + "@"
                        + str(index),
                        file_name=file_name.split(".")[0],
                        timestamp=timestamp,
                        status=action["status"],
                        when=action["when"],
                        limit=action.get("limit"),
                        files_requested=action.get("files_requested"),
                        store_id=action.get("store_id"),
                        files_names_to_scrape=action.get("files_names_to_scrape"),
                        when_date=action["when_date"],
                        filter_null=action["filter_null"],
                        filter_zero=action["filter_zero"],
                        suppress_exception=action["suppress_exception"],
                    ).to_dict()
                )
            pushed_timestamp.append(timestamp)

        local_cahce.update_pushed_timestamps(file_name, pushed_timestamp)

        self.uploader._insert_to_database(ScraperStatus.get_table_name(), records)

    def _push_files_data(self, local_cahce:CacheState):
        #
        for file in os.listdir(self.outputs_folder):
            if not file.endswith(".csv"):
                logging.warn(f"Skipping '{file}', should we store it?")
                continue
            
            large_file_pusher = LargeFilePushManager(self.outputs_folder, self.uploader)
            large_file_pusher.process_file(file, local_cahce)
            
        logging.info("Files data pushed in DynamoDB successfully.")

    def upload(self,force_restart=False):
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
