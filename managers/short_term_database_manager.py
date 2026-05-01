import hashlib
import os
import pandas as pd
import json
from utils import Logger
from remotes import ShortTermDatabaseUploader
from managers.cache_manager import CacheManager, CacheState
from managers.large_file_push_manager import LargeFilePushManager
from il_supermarket_parsers import ParserStatusOutput
from il_supermarket_scarper import ScraperStatusOutput
from typing import Union


class ShortTermDBDatasetManager:
    def __init__(
        self,
        app_folder,
        outputs_folder,
        status_folder,
        short_term_db_target: ShortTermDatabaseUploader,
        enabled_scrapers: list[str],
        enabled_file_types: list[str],
        scraping_status_folder,
        converting_status_folder,
    ):
        self.app_folder = app_folder
        self.uploader = short_term_db_target
        self.outputs_folder = outputs_folder
        self.status_folder = status_folder
        self.enabled_scrapers = enabled_scrapers
        self.enabled_file_types = enabled_file_types
        self.scraping_status_folder = scraping_status_folder
        self.converting_status_folder = converting_status_folder

    @staticmethod
    def _status_event_index(event_json: dict, source_file: str) -> str:
        """Stable document id for status rows (partition key in short-term DB)."""
        canonical = json.dumps(event_json, sort_keys=True, default=str)
        digest = hashlib.sha256(f"{source_file}\0{canonical}".encode()).hexdigest()
        return digest

    def _push_a_status_files(
        self,
        status_folder,
        model_type: Union[ParserStatusOutput, ScraperStatusOutput],
        target_table: str,
        local_cahce: CacheState,
    ):
        for file_name in os.listdir(status_folder):
            if file_name.endswith(".json"):

                pushed_ids = list(local_cahce.get_pushed_timestamps(file_name))
                pushed_set = set(pushed_ids)
                added_ids: list = []
                processed_events = []

                with open(os.path.join(status_folder, file_name), "r") as file:
                    records = json.load(file)

                    model = model_type(**records)

                    for event in model.events:
                        try:
                            event_json = json.loads(event.model_dump_json())
                            row_index = self._status_event_index(
                                event_json, file_name
                            )
                        except Exception as e:
                            Logger.error(f"Error processing event: {e}")
                            continue
                        if row_index in pushed_set:
                            continue
                        processed_events.append(
                            {"index": row_index, **event_json}
                        )
                        pushed_set.add(row_index)
                        added_ids.append(row_index)
                    self.uploader._insert_to_destinations(
                        target_table, processed_events
                    )

                merged = pushed_ids + [i for i in added_ids if i not in pushed_ids]
                local_cahce.update_pushed_timestamps(file_name, merged)

    def _push_parser_status(self, local_cahce: CacheState):
        self._push_a_status_files(
            self.converting_status_folder,
            ParserStatusOutput,
            "ParserStatus",
            local_cahce,
        )
        Logger.info("Parser status stored in DynamoDB successfully.")

    def _push_scraper_status(self, local_cahce: CacheState):
        self._push_a_status_files(
            self.scraping_status_folder,
            ScraperStatusOutput,
            "ScraperStatus",
            local_cahce,
        )
        Logger.info("Scraper status stored in DynamoDB successfully.")

    def _push_status_files(self, local_cahce: CacheState):
        self._push_scraper_status(local_cahce)

        self._push_parser_status(local_cahce)

    def _push_files_data(self, local_cahce: CacheState):
        #
        for file in os.listdir(self.outputs_folder):
            if not file.endswith(".csv"):
                Logger.warning(f"Skipping '{file}', should we store it?")
                continue

            large_file_pusher = LargeFilePushManager(self.outputs_folder, self.uploader)
            large_file_pusher.process_file(file, local_cahce)

        Logger.info("Files data pushed in DynamoDB successfully.")

    def upload(self, force_restart=False):
        """
        Upload the data to the database.
        """
        with CacheManager(self.app_folder) as local_cache:
            if local_cache.is_empty() or force_restart:
                self.uploader.restart_database(
                    self.enabled_scrapers, self.enabled_file_types
                )

            self._push_files_data(local_cache)
            self._push_status_files(local_cache)

        Logger.info("Upload completed successfully.")
