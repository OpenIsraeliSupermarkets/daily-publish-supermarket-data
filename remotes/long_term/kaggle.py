"""Module for handling Kaggle dataset uploads and management.

This module provides functionality to upload, update and manage datasets on Kaggle,
specifically designed for supermarket data management.
"""

import time
import os
import re
import pytz
from utils import Logger
import shutil
import json
import tempfile
import zipfile
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from .base import LongTermDatabaseUploader
from il_supermarket_scarper import DumpFolderNames, ScraperFactory

KAGGLEHUB_AVAILABLE = None
try:
    import kagglehub
    from kagglehub.clients import build_kaggle_client
    from kagglesdk.datasets.types.dataset_api_service import (
        ApiGetDatasetRequest,
        ApiListDatasetFilesRequest,
    )
except Exception as e:
    KAGGLEHUB_AVAILABLE = e

# kagglehub zips the whole folder when file count exceeds this (no parallel upload).
KAGGLE_MAX_FILES_FOR_PARALLEL_UPLOAD = 50


class KaggleUploader(LongTermDatabaseUploader):
    """Handles uploading and managing datasets on Kaggle.

    This class provides methods to upload data to Kaggle, manage dataset versions,
    and check update status of datasets.
    """

    def __init__(self, dataset_remote_name, when, dataset_path=None):
        """Initialize the Kaggle uploader.

        Args:
            dataset_remote_name (str): Full Kaggle dataset handle (e.g. 'username/dataset-name')
            when (datetime): Timestamp for the dataset
            dataset_path (str, optional): Local path for staging files.
                Defaults to the dataset name part of the handle.
        """
        if dataset_path is None:
            dataset_path = dataset_remote_name.split("/")[-1]
        super().__init__(dataset_path, when)
        self.dataset_remote_name = dataset_remote_name
        self.when = when

        if KAGGLEHUB_AVAILABLE is not None:
            raise ImportError("Failed to import kagglehub: \n%s" % KAGGLEHUB_AVAILABLE)

        Logger.info(f"Kaggle dataset handle: {self.dataset_remote_name}")

    @staticmethod
    def _normalize_name(value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", value.lower())

    def _scraper_prefixes(self):
        """Return (normalized_prefix, zip_stem) pairs per scraper, longest first."""
        prefixes = []
        for scraper in ScraperFactory.all_scrapers_name():
            dump_name = DumpFolderNames[scraper].value
            stem = dump_name.lower()
            prefixes.append((self._normalize_name(dump_name), stem))
        prefixes.sort(key=lambda item: len(item[0]), reverse=True)
        return prefixes

    def _group_files_by_scraper(self, filenames):
        """Group staged filenames into one bucket per scraper.

        Matches dump-folder prefixes at the start or end of the filename stem so both
        `bareket_price_file.json` and `price_file_bareket.csv` map to the same zip.
        """
        groups = defaultdict(list)
        prefixes = self._scraper_prefixes()
        for filename in filenames:
            normalized = self._normalize_name(os.path.splitext(filename)[0])
            matched_stem = None
            for prefix, stem in prefixes:
                if (
                    normalized == prefix
                    or normalized.startswith(prefix)
                    or normalized.endswith(prefix)
                ):
                    matched_stem = stem
                    break
            groups[matched_stem or "misc"].append(filename)
        return groups

    def pack_staged_files(self):
        """Pack staged files into one zip per scraper during compose.

        kagglehub only parallelizes uploads when there are <= 50 files. Above that it
        builds one giant archive.zip. Packing here keeps index.json and one zip per
        scraper so dataset_upload can use parallel blob uploads.
        """
        if not os.path.isdir(self.dataset_path):
            return

        staged_files = sorted(
            name
            for name in os.listdir(self.dataset_path)
            if os.path.isfile(os.path.join(self.dataset_path, name))
        )
        keep_as_is = {"index.json"}
        to_pack = [
            name
            for name in staged_files
            if name not in keep_as_is and not name.endswith(".zip")
        ]
        if not to_pack:
            return

        if len(staged_files) <= KAGGLE_MAX_FILES_FOR_PARALLEL_UPLOAD:
            Logger.info(
                "Staged file count (%s) within Kaggle parallel limit; packing per scraper anyway",
                len(staged_files),
            )

        groups = self._group_files_by_scraper(to_pack)
        Logger.info(
            "Packing %s staged files into %s per-scraper zip archives",
            len(to_pack),
            len(groups),
        )

        for stem, filenames in groups.items():
            zip_name = f"{stem}.zip"
            zip_path = os.path.join(self.dataset_path, zip_name)
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
                for filename in filenames:
                    file_path = os.path.join(self.dataset_path, filename)
                    zipf.write(file_path, arcname=filename)
            for filename in filenames:
                os.remove(os.path.join(self.dataset_path, filename))

        packed_count = len(
            [
                name
                for name in os.listdir(self.dataset_path)
                if os.path.isfile(os.path.join(self.dataset_path, name))
            ]
        )
        Logger.info("Staged dataset now has %s files after per-scraper packing", packed_count)
        if packed_count > KAGGLE_MAX_FILES_FOR_PARALLEL_UPLOAD:
            Logger.warning(
                "Packed file count (%s) still exceeds Kaggle parallel limit (%s)",
                packed_count,
                KAGGLE_MAX_FILES_FOR_PARALLEL_UPLOAD,
            )

    def _sync_n_load_index(self):
        """Sync the index of the dataset."""
        try:
            index_local = os.path.join(self.dataset_path, "index.json")
            if not os.path.exists(index_local):
                downloaded = kagglehub.dataset_download(
                    self.dataset_remote_name,
                    path="index.json",
                    force_download=True,
                )
                os.makedirs(self.dataset_path, exist_ok=True)
                shutil.copy2(downloaded, index_local)
            else:
                Logger.warning("Index file already exists")

            with open(index_local, "r", encoding="utf-8") as file:
                index = json.load(file)
            return index
        except Exception as e:
            error_str = str(e).lower()
            if "404" in error_str or "not found" in error_str:
                return None
            raise Exception("Error connecting to Kaggle: %s" % e)

    def get_current_index(self):
        """Get the current index of the dataset.

        Returns:
            int: The current index of the dataset
        """
        index = self._sync_n_load_index()
        return self._read_index(index)

    def increase_index(self):
        """Download and update the dataset index from Kaggle."""
        index = self._sync_n_load_index()
        index = self._increase_index(index)

        os.makedirs(self.dataset_path, exist_ok=True)
        with open(
            os.path.join(self.dataset_path, "index.json"), "w+", encoding="utf-8"
        ) as file:
            json.dump(index, file)

    def upload_to_dataset(self, message, **additional_metadata):
        """Upload a new version of the dataset.

        Args:
            message (str): Version notes for the upload
        """
        os.makedirs(self.dataset_path, exist_ok=True)
        kagglehub.dataset_upload(
            self.dataset_remote_name,
            self.dataset_path,
            version_notes=message,
        )
        time.sleep(3)  # wait for kaggle to process the request.

    def fetch_file(self, file_name, dest_dir):
        """Download a dataset file from Kaggle into dest_dir."""
        os.makedirs(dest_dir, exist_ok=True)
        downloaded = kagglehub.dataset_download(
            self.dataset_remote_name,
            path=file_name,
            force_download=True,
        )
        local_path = (
            downloaded
            if os.path.isfile(downloaded)
            else os.path.join(downloaded, file_name)
        )
        destination = os.path.join(dest_dir, os.path.basename(file_name))
        shutil.copy2(local_path, destination)
        return destination

    def clean(self):
        """Clean up temporary files."""
        shutil.rmtree(self.dataset_path)
        super().clean()

    def was_updated_in_last(self, seconds: int = 24 * 60 * 60) -> bool:
        """Check if the dataset was updated within specified seconds.

        Args:
            seconds (int, optional): Number of seconds to look back. Defaults to 24*60*60.

        Returns:
            bool: True if updated within specified seconds, False otherwise
        """
        try:
            owner, dataset_name = self.dataset_remote_name.split("/", 1)
            with build_kaggle_client() as api_client:
                r = ApiGetDatasetRequest()
                r.owner_slug = owner
                r.dataset_slug = dataset_name
                dataset = api_client.datasets.dataset_api_client.get_dataset(r)
            last_updated = dataset.last_updated
            if not last_updated:
                return False
            if last_updated.tzinfo is None:
                last_updated = last_updated.replace(tzinfo=pytz.utc)
            return (datetime.now(tz=pytz.utc) - last_updated) < timedelta(
                seconds=seconds
            )
        except Exception as e:
            Logger.error("Error checking Kaggle dataset update time: %s", str(e))
            return False

    def list_files(self, chain=None, extension=None):
        """List all files in the dataset.

        Args:
            chain (str, optional): Filter files by chain name. Defaults to None.
            extension (str, optional): Filter files by extension. Defaults to None.

        Returns:
            list: List of file paths in the dataset
        """
        try:
            owner, dataset_name = self.dataset_remote_name.split("/", 1)
            page_token = None
            collected_files = []
            with build_kaggle_client() as api_client:
                while True:
                    r = ApiListDatasetFilesRequest()
                    r.owner_slug = owner
                    r.dataset_slug = dataset_name
                    if page_token:
                        r.page_token = page_token
                    data = api_client.datasets.dataset_api_client.list_dataset_files(r)
                    collected_files.extend([f.name for f in (data.dataset_files or [])])
                    page_token = data.next_page_token
                    if not page_token:
                        break

            if chain is not None or extension is not None:
                pattern = self._build_pattern(chain, extension)
                collected_files = [
                    f
                    for f in collected_files
                    if re.match(pattern.replace("*", ".*"), f)
                ]
            return collected_files
        except Exception as e:
            Logger.error("Error listing files from Kaggle: %s", str(e))
            return []

    def get_file_content(self, file_name):
        """Get the content of a specific file from the dataset.

        Args:
            file_name (str): Name of the file to retrieve

        Returns:
            pandas.DataFrame or dict or str: Content of the file
        """
        tmp_dir = tempfile.mkdtemp()
        try:
            downloaded = kagglehub.dataset_download(
                self.dataset_remote_name,
                path=file_name,
                force_download=True,
            )
            local_path = (
                downloaded
                if os.path.isfile(downloaded)
                else os.path.join(downloaded, file_name)
            )

            if file_name.endswith(".json"):
                with open(local_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            elif file_name.endswith(".csv"):
                return pd.read_csv(local_path)
            else:
                with open(local_path, "r") as f:
                    return f.read()
        except Exception as e:
            Logger.error("Error getting file content from Kaggle: %s", str(e))
            raise e
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def download(self):
        """Download the data from the remote dataset."""
        shutil.rmtree(self.dataset_path, ignore_errors=True)
        downloaded = kagglehub.dataset_download(
            self.dataset_remote_name,
            force_download=True,
        )
        shutil.copytree(downloaded, self.dataset_path, dirs_exist_ok=True)
        return self.dataset_path
