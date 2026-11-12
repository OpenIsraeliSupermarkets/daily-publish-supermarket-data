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
import pandas as pd
from datetime import datetime, timedelta
from .base import LongTermDatabaseUploader

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


def _env_int(name, default):
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _env_float(name, default):
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


# Kaggle throttles per account. CI system tests share the prod account, so a
# few back-to-back runs can push the prod publish into 429s (#167).
KAGGLE_RETRY_ATTEMPTS = _env_int("KAGGLE_RETRY_ATTEMPTS", 6)
KAGGLE_RETRY_BASE_DELAY = _env_float("KAGGLE_RETRY_BASE_DELAY", 30.0)
KAGGLE_RETRY_MAX_DELAY = _env_float("KAGGLE_RETRY_MAX_DELAY", 300.0)


def _http_status(exc):
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None)


def is_rate_limited(exc):
    """True if exc is Kaggle's HTTP 429 Too Many Requests."""
    if _http_status(exc) == 429:
        return True
    text = str(exc).lower()
    return "429" in text and "too many requests" in text


def _retry_after_seconds(exc):
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None) or {}
    try:
        value = headers.get("Retry-After")
    except AttributeError:
        return None
    try:
        return max(0.0, float(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def retry_on_rate_limit(
    fn,
    what,
    attempts=None,
    base_delay=None,
    max_delay=None,
    sleep=None,
):
    """Call fn(), retrying with exponential backoff while Kaggle answers 429.

    Other errors are raised on the first try. After the last attempt the
    original 429 error is raised.
    """
    attempts = KAGGLE_RETRY_ATTEMPTS if attempts is None else attempts
    base_delay = KAGGLE_RETRY_BASE_DELAY if base_delay is None else base_delay
    max_delay = KAGGLE_RETRY_MAX_DELAY if max_delay is None else max_delay
    sleep = time.sleep if sleep is None else sleep
    attempts = max(1, attempts)
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:  # pylint: disable=broad-except
            if not is_rate_limited(e) or attempt == attempts:
                raise
            delay = _retry_after_seconds(e)
            if delay is None:
                delay = base_delay * (2 ** (attempt - 1))
            delay = min(delay, max_delay)
            Logger.warning(
                f"Kaggle rate limit (429) on {what}, attempt {attempt}/{attempts}; "
                f"retrying in {delay:.0f}s"
            )
            sleep(delay)
    return None  # unreachable


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

    def _dataset_download(self, path=None):
        """kagglehub.dataset_download with retry on 429."""
        kwargs = {"force_download": True}
        if path is not None:
            kwargs["path"] = path
        what = f"download {path or 'dataset'}"
        return retry_on_rate_limit(
            lambda: kagglehub.dataset_download(self.dataset_remote_name, **kwargs),
            what,
        )

    def _sync_n_load_index(self):
        """Sync the index of the dataset."""
        try:
            index_local = os.path.join(self.dataset_path, "index.json")
            if not os.path.exists(index_local):
                downloaded = self._dataset_download("index.json")
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
        retry_on_rate_limit(
            lambda: kagglehub.dataset_upload(
                self.dataset_remote_name,
                self.dataset_path,
                version_notes=message,
            ),
            "upload",
        )
        time.sleep(3)  # wait for kaggle to process the request.

    def fetch_file(self, file_name, dest_dir):
        """Download a dataset file from Kaggle into dest_dir."""
        os.makedirs(dest_dir, exist_ok=True)
        downloaded = self._dataset_download(file_name)
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
            downloaded = self._dataset_download(file_name)
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
        downloaded = self._dataset_download()
        shutil.copytree(downloaded, self.dataset_path, dirs_exist_ok=True)
        return self.dataset_path
