import os
import zipfile
from datetime import datetime
from unittest.mock import patch

import pytest
from il_supermarket_scarper import DumpFolderNames

from remotes.long_term.kaggle import KaggleUploader


@pytest.fixture
def kaggle_uploader(tmp_path):
    with patch("remotes.long_term.kaggle.KAGGLEHUB_AVAILABLE", None):
        uploader = KaggleUploader(
            dataset_remote_name="erlichsefi/test-super-dataset-2",
            when=datetime.now(),
            dataset_path=str(tmp_path / "dataset"),
        )
    os.makedirs(uploader.dataset_path, exist_ok=True)
    return uploader


def _write(path, content="x"):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)


def test_group_files_by_scraper(kaggle_uploader):
    bareket = DumpFolderNames["BAREKET"].value.lower()
    shufersal = DumpFolderNames["SHUFERSAL"].value.lower()
    groups = kaggle_uploader._group_files_by_scraper(
        [
            f"{bareket}.json",
            f"{bareket}_price_file.json",
            f"price_file_{bareket}.csv",
            f"{shufersal}_promo_file.json",
            "parser-status.json",
        ]
    )
    assert set(groups[bareket]) == {
        f"{bareket}.json",
        f"{bareket}_price_file.json",
        f"price_file_{bareket}.csv",
    }
    assert groups[shufersal] == [f"{shufersal}_promo_file.json"]
    assert groups["misc"] == ["parser-status.json"]


def test_pack_staged_files_creates_one_zip_per_scraper(kaggle_uploader):
    bareket = DumpFolderNames["BAREKET"].value.lower()
    shufersal = DumpFolderNames["SHUFERSAL"].value.lower()
    dataset = kaggle_uploader.dataset_path

    _write(os.path.join(dataset, "index.json"), '{"0":"2026-01-01"}')
    _write(os.path.join(dataset, f"{bareket}.json"))
    _write(os.path.join(dataset, f"{bareket}_price_file.json"))
    _write(os.path.join(dataset, f"price_file_{bareket}.csv"))
    _write(os.path.join(dataset, f"{shufersal}_promo_file.json"))
    _write(os.path.join(dataset, "parser-status.json"))

    kaggle_uploader.pack_staged_files()

    remaining = sorted(os.listdir(dataset))
    assert remaining == sorted(
        ["index.json", f"{bareket}.zip", f"{shufersal}.zip", "misc.zip"]
    )

    with zipfile.ZipFile(os.path.join(dataset, f"{bareket}.zip")) as zipf:
        assert sorted(zipf.namelist()) == sorted(
            [
                f"{bareket}.json",
                f"{bareket}_price_file.json",
                f"price_file_{bareket}.csv",
            ]
        )
    with zipfile.ZipFile(os.path.join(dataset, f"{shufersal}.zip")) as zipf:
        assert zipf.namelist() == [f"{shufersal}_promo_file.json"]
    with zipfile.ZipFile(os.path.join(dataset, "misc.zip")) as zipf:
        assert zipf.namelist() == ["parser-status.json"]


def test_pack_staged_files_noop_when_only_index(kaggle_uploader):
    _write(os.path.join(kaggle_uploader.dataset_path, "index.json"), "{}")
    kaggle_uploader.pack_staged_files()
    assert os.listdir(kaggle_uploader.dataset_path) == ["index.json"]
