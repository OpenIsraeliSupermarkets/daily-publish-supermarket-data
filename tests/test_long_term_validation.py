import json
import os
import zipfile
import tempfile
from datetime import datetime

from il_supermarket_scarper import DumpFolderNames, FileTypesFilters

from managers.quality_indicators import (
    PARSER_QUALITY_FILENAME,
    SCRAPER_QUALITY_FILENAME,
)
from remotes.long_term.file_storage import DummyFileStorage
from tests.validation_utils import validate_long_term_structure


def _build_packed_remote(remote_dir, scrapers):
    os.makedirs(remote_dir, exist_ok=True)
    with open(os.path.join(remote_dir, "index.json"), "w", encoding="utf-8") as handle:
        handle.write('{"0":"2026-01-01"}')

    for scraper in scrapers:
        stem = DumpFolderNames[scraper].value.lower()
        members = [f"{stem}.json"]
        for file_type in FileTypesFilters:
            members.append(f"{scraper.lower()}_{file_type.name.lower()}.json")
        members.append(f"price_file_{scraper.lower()}.csv")

        zip_path = os.path.join(remote_dir, f"{stem}.zip")
        with zipfile.ZipFile(zip_path, "w") as zipf:
            for member in members:
                zipf.writestr(member, "test")


def test_validate_long_term_structure_packed_layout():
    scrapers = ["BAREKET", "WOLT"]
    with tempfile.TemporaryDirectory() as temp_dir:
        remote_dir = os.path.join(temp_dir, "remote")
        stage_dir = os.path.join(temp_dir, "stage")
        _build_packed_remote(remote_dir, scrapers)

        uploader = DummyFileStorage(
            dataset_path=stage_dir,
            dataset_remote_path=remote_dir,
            when=datetime.now(),
        )
        validate_long_term_structure(
            uploader, stage_dir, scrapers, in_app=False
        )


def test_validate_long_term_structure_flat_layout():
    scrapers = ["BAREKET"]
    with tempfile.TemporaryDirectory() as temp_dir:
        remote_dir = os.path.join(temp_dir, "remote")
        stage_dir = os.path.join(temp_dir, "stage")
        os.makedirs(remote_dir, exist_ok=True)

        with open(os.path.join(remote_dir, "index.json"), "w", encoding="utf-8") as handle:
            handle.write('{"0":"2026-01-01"}')
        stem = DumpFolderNames["BAREKET"].value.lower()
        with open(os.path.join(remote_dir, f"{stem}.json"), "w", encoding="utf-8") as handle:
            handle.write("{}")
        for file_type in FileTypesFilters:
            name = f"bareket_{file_type.name.lower()}.json"
            with open(os.path.join(remote_dir, name), "w", encoding="utf-8") as handle:
                handle.write("{}")
        with open(
            os.path.join(remote_dir, "price_file_bareket.csv"), "w", encoding="utf-8"
        ) as handle:
            handle.write("a,b\n1,2\n")

        uploader = DummyFileStorage(
            dataset_path=stage_dir,
            dataset_remote_path=remote_dir,
            when=datetime.now(),
        )
        validate_long_term_structure(
            uploader, stage_dir, scrapers, in_app=False
        )


def test_validate_long_term_structure_with_quality_files():
    scrapers = ["BAREKET"]
    with tempfile.TemporaryDirectory() as temp_dir:
        remote_dir = os.path.join(temp_dir, "remote")
        stage_dir = os.path.join(temp_dir, "stage")
        os.makedirs(remote_dir, exist_ok=True)

        with open(os.path.join(remote_dir, "index.json"), "w", encoding="utf-8") as handle:
            handle.write('{"0":"2026-01-01"}')
        for quality_file in (SCRAPER_QUALITY_FILENAME, PARSER_QUALITY_FILENAME):
            with open(os.path.join(remote_dir, quality_file), "w", encoding="utf-8") as handle:
                json.dump({"computed_at": "2026-01-01", "iterations": []}, handle)

        stem = DumpFolderNames["BAREKET"].value.lower()
        with open(os.path.join(remote_dir, f"{stem}.json"), "w", encoding="utf-8") as handle:
            handle.write("{}")
        for file_type in FileTypesFilters:
            name = f"bareket_{file_type.name.lower()}.json"
            with open(os.path.join(remote_dir, name), "w", encoding="utf-8") as handle:
                handle.write("{}")
        with open(
            os.path.join(remote_dir, "price_file_bareket.csv"), "w", encoding="utf-8"
        ) as handle:
            handle.write("a,b\n1,2\n")

        uploader = DummyFileStorage(
            dataset_path=stage_dir,
            dataset_remote_path=remote_dir,
            when=datetime.now(),
        )
        validate_long_term_structure(
            uploader, stage_dir, scrapers, in_app=False
        )


class _ListingStub:
    """Minimal remote stub that returns Kaggle-style relative paths."""

    def __init__(self, files):
        self._files = list(files)

    def was_updated_in_last(self, seconds=24 * 60 * 60):
        return True

    def list_files(self, chain=None, extension=None):
        files = self._files
        if extension is not None:
            files = [name for name in files if name.endswith(extension)]
        return files


def test_validate_long_term_structure_kaggle_expanded_layout():
    """Kaggle expands uploaded {scraper}.zip into {scraper}/member paths."""
    scrapers = ["WOLT"]
    stem = DumpFolderNames["WOLT"].value.lower()
    files = ["index.json", f"{stem}/{stem}.json", f"{stem}/store_file_wolt.csv"]
    for file_type in FileTypesFilters:
        files.append(f"{stem}/wolt_{file_type.name.lower()}.json")

    with tempfile.TemporaryDirectory() as temp_dir:
        stage_dir = os.path.join(temp_dir, "stage")
        validate_long_term_structure(
            _ListingStub(files), stage_dir, scrapers, in_app=False
        )
