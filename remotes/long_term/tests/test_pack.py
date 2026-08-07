import os
import zipfile
from datetime import datetime

import pytest
from il_supermarket_scarper import DumpFolderNames

from remotes.long_term.file_storage import DummyFileStorage
from remotes.long_term.packer import StagedDatasetPacker


@pytest.fixture
def long_term_uploader(tmp_path):
    remote = tmp_path / "remote"
    remote.mkdir()
    uploader = DummyFileStorage(
        dataset_path=str(tmp_path / "dataset"),
        dataset_remote_path=str(remote),
        when=datetime.now(),
    )
    os.makedirs(uploader.dataset_path, exist_ok=True)
    return uploader


@pytest.fixture
def packer():
    return StagedDatasetPacker()


def _write(path, content="x"):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)


def test_group_files_by_scraper(packer):
    bareket = DumpFolderNames["BAREKET"].value.lower()
    shufersal = DumpFolderNames["SHUFERSAL"].value.lower()
    groups = packer.group_files_by_scraper(
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


def test_pack_staged_files_creates_one_zip_per_scraper(long_term_uploader):
    bareket = DumpFolderNames["BAREKET"].value.lower()
    shufersal = DumpFolderNames["SHUFERSAL"].value.lower()
    dataset = long_term_uploader.dataset_path

    _write(os.path.join(dataset, "index.json"), '{"0":"2026-01-01"}')
    _write(os.path.join(dataset, f"{bareket}.json"))
    _write(os.path.join(dataset, f"{bareket}_price_file.json"))
    _write(os.path.join(dataset, f"price_file_{bareket}.csv"))
    _write(os.path.join(dataset, f"{shufersal}_promo_file.json"))
    _write(os.path.join(dataset, "parser-status.json"))

    long_term_uploader.pack_staged_files()

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


def test_pack_staged_files_noop_when_only_index(long_term_uploader):
    _write(os.path.join(long_term_uploader.dataset_path, "index.json"), "{}")
    long_term_uploader.pack_staged_files()
    assert os.listdir(long_term_uploader.dataset_path) == ["index.json"]


def test_fetch_file_moves_instead_of_copying(long_term_uploader, tmp_path):
    remote_file = os.path.join(long_term_uploader.dataset_remote_path, "bareket.zip")
    _write(remote_file, "zip-bytes")
    dest_dir = str(tmp_path / "dest")

    local_path = long_term_uploader.fetch_file("bareket.zip", dest_dir)

    assert local_path == os.path.join(dest_dir, "bareket.zip")
    assert os.path.isfile(local_path)
    assert not os.path.exists(remote_file)


def test_unpack_zip_extracts_members(packer, tmp_path):
    zip_path = tmp_path / "bareket.zip"
    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.writestr("bareket.json", "{}")
        zipf.writestr("price_file_bareket.csv", "a,b\n1,2\n")

    dest_dir = tmp_path / "out"
    dest_dir.mkdir()
    members = packer.unpack_zip(str(zip_path), str(dest_dir))

    assert sorted(members) == ["bareket.json", "price_file_bareket.csv"]
    assert (dest_dir / "bareket.json").is_file()
    assert (dest_dir / "price_file_bareket.csv").is_file()


def test_unpack_directory_removes_zips(packer, tmp_path):
    zip_path = tmp_path / "misc.zip"
    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.writestr("parser-status.json", "{}")

    members = packer.unpack(str(tmp_path), remove_zips=True)

    assert members == ["parser-status.json"]
    assert (tmp_path / "parser-status.json").is_file()
    assert not zip_path.exists()


def test_unpack_files_fetches_and_extracts(long_term_uploader, tmp_path):
    remote_zip = os.path.join(long_term_uploader.dataset_remote_path, "bareket.zip")
    with zipfile.ZipFile(remote_zip, "w") as zipf:
        zipf.writestr("bareket.json", "{}")

    dest_dir = str(tmp_path / "dest")
    members = long_term_uploader.unpack_files(["bareket.zip"], dest_dir)

    assert members == ["bareket.json"]
    assert os.path.isfile(os.path.join(dest_dir, "bareket.json"))
    assert not os.path.exists(remote_zip)
