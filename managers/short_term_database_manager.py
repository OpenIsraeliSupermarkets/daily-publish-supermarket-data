import hashlib
import os
import pandas as pd
import json
from utils import Logger
from remotes import ShortTermDatabaseUploader
from managers.cache_manager import CacheManager, CacheState
from managers.large_file_push_manager import LargeFilePushManager
from il_supermarket_parsers import ParserStatusOutput
from il_supermarket_scarper import DumpFolderNames, ScraperStatusOutput
from typing import Union

from utils.mongo_bson import sanitize_for_mongo
from data_models.raw_schema import PARSER_STAGE, SCRAPER_STAGE, status_table_name


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

    def _chain_of_status_file(self, file_name: str) -> str:
        """Chain a status file belongs to, from its name alone: scraper files
        are named by the chain's dump folder (``ramilevy.json``), parser files
        ``<chain>_<file_type>.json``. Both give the scraper's name
        (``rami_levy``), so one chain's stages share it (#139). Chain names
        contain ``_``, so match the enabled scrapers, longest first."""
        stem = file_name[: -len(".json")].lower()
        for scraper in self.enabled_scrapers:
            folder = DumpFolderNames.__members__.get(scraper.upper())
            if folder is not None and stem == folder.value.lower():
                return scraper.lower()
        for chain in sorted(
            (c.lower() for c in self.enabled_scrapers), key=len, reverse=True
        ):
            if stem == chain or stem.startswith(chain + "_"):
                return chain
        return stem

    def _push_status_rows(
        self,
        rows,
        envelope: dict,
        pushed_set: set,
        added_ids: list,
    ):
        """Insert not-yet-pushed status rows of one section, each wrapped in
        ``envelope``, into ``status_<stage>_<section>``."""
        processed = []
        for row in rows:
            try:
                row_json = json.loads(row.model_dump_json())
                row_index = self._status_event_index(
                    row_json, envelope["source_status_file"]
                )
            except Exception as e:
                Logger.error(f"Error processing event: {e}")
                continue
            if row_index in pushed_set:
                continue
            processed.append(
                sanitize_for_mongo({"index": row_index, **envelope, "data": row_json})
            )
            pushed_set.add(row_index)
            added_ids.append(row_index)
        if not processed:
            return
        self.uploader._insert_to_destinations(
            status_table_name(envelope["stage"], envelope["section"]), processed
        )

    def _push_a_status_files(
        self,
        status_folder,
        model_type: Union[ParserStatusOutput, ScraperStatusOutput],
        stage: str,
        local_cahce: CacheState,
    ):
        """Validate each status file with its model, then push every list the
        model has, one destination per section (#127). A section added to the
        model upstream is pushed without a change here."""
        for file_name in sorted(os.listdir(status_folder)):
            if not file_name.endswith(".json"):
                continue

            pushed_ids = list(local_cahce.get_pushed_timestamps(file_name))
            pushed_set = set(pushed_ids)
            added_ids: list = []

            with open(os.path.join(status_folder, file_name), "r") as file:
                model = model_type(**json.load(file))

            chain = self._chain_of_status_file(file_name)
            for section in type(model).model_fields:
                rows = getattr(model, section)
                if not isinstance(rows, list):
                    continue
                envelope = {
                    "chain": chain,
                    "stage": stage,
                    "section": section,
                    "source_status_file": file_name,
                }
                self._push_status_rows(rows, envelope, pushed_set, added_ids)

            merged = pushed_ids + [i for i in added_ids if i not in pushed_ids]
            local_cahce.update_pushed_timestamps(file_name, merged)

    def _push_status_files(self, local_cahce: CacheState):
        self._push_a_status_files(
            self.scraping_status_folder,
            ScraperStatusOutput,
            SCRAPER_STAGE,
            local_cahce,
        )
        self._push_a_status_files(
            self.converting_status_folder,
            ParserStatusOutput,
            PARSER_STAGE,
            local_cahce,
        )
        Logger.info("Scraper and parser status stored successfully.")

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
