import os
import zipfile
import tempfile
from datetime import datetime

from il_supermarket_scarper import DumpFolderNames, FileTypesFilters

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
