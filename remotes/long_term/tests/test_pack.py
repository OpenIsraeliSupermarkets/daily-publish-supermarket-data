import os
import zipfile
from datetime import datetime

import pytest
from il_supermarket_scarper import DumpFolderNames, ScraperFactory

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


def test_group_files_by_scraper_name_aliases_and_temp_suffix(packer):
    """Files use scraper enum spelling; zips use DumpFolderNames stems."""
    meshmat_zip = DumpFolderNames["MESHMAT_YOSEF_1"].value.lower()
    yayno_zip = DumpFolderNames["YAYNO_BITAN_AND_CARREFOUR"].value.lower()
    hazi_zip = DumpFolderNames["HAZI_HINAM"].value.lower()
    keshet_zip = DumpFolderNames["KESHET"].value.lower()
    rami_zip = DumpFolderNames["RAMI_LEVY"].value.lower()

    groups = packer.group_files_by_scraper(
        [
            "meshmat_yosef_1_price_file.json",
            "price_file_meshmat_yosef_2.csv",
            "yayno_bitan_and_carrefour_store_file.json",
            "price_full_file_yayno_bitan_and_carrefour.csv",
            "promo_full_file_hazi_hinam_temp.csv",
            "promo_full_file_keshet_temp.csv",
            "promo_full_file_rami_levy_temp.csv",
            "parser-status.json",
        ]
    )

    assert groups[meshmat_zip] == ["meshmat_yosef_1_price_file.json"]
    assert groups[DumpFolderNames["MESHMAT_YOSEF_2"].value.lower()] == [
        "price_file_meshmat_yosef_2.csv"
    ]
    assert set(groups[yayno_zip]) == {
        "yayno_bitan_and_carrefour_store_file.json",
        "price_full_file_yayno_bitan_and_carrefour.csv",
    }
    assert groups[hazi_zip] == ["promo_full_file_hazi_hinam_temp.csv"]
    assert groups[keshet_zip] == ["promo_full_file_keshet_temp.csv"]
    assert groups[rami_zip] == ["promo_full_file_rami_levy_temp.csv"]
    assert groups["misc"] == ["parser-status.json"]


def test_group_files_for_chains_filtered_as_unstable(packer, monkeypatch):
    """Chains omitted by all_scrapers_name() still get their own zip.

    Before 08:00 Asia/Jerusalem, all_scrapers_name() drops the laibcatalog
    chains whose catalogs are empty overnight. Their staged files must not
    fall through to misc.
    """
    monkeypatch.setattr(
        ScraperFactory,
        "all_scrapers_name",
        lambda *args, **kwargs: ["BAREKET"],
    )
    victory = DumpFolderNames["VICTORY_NEW_SOURCE"].value.lower()
    het_cohen = DumpFolderNames["HET_COHEN_NEW_SOURCE"].value.lower()
    mahsani = DumpFolderNames["MAHSANI_ASHUK_NEW_SOURCE"].value.lower()
    legacy_victory = DumpFolderNames["VICTORY"].value.lower()

    groups = packer.group_files_by_scraper(
        [
            "price_full_file_victory_new_source.csv",
            "victory_new_source_price_full_file.json",
            f"{victory}.json",
            "promo_full_file_het_cohen_new_source.csv",
            "het_cohen_new_source_store_file.json",
            f"{het_cohen}.json",
            "price_file_mahsani_ashuk_new_source.csv",
            "mahsani_ashuk_new_source_promo_file.json",
            f"{mahsani}.json",
            "price_full_file_victory.csv",
            "parser-status.json",
        ]
    )

    assert set(groups[victory]) == {
        "price_full_file_victory_new_source.csv",
        "victory_new_source_price_full_file.json",
        f"{victory}.json",
    }
    assert set(groups[het_cohen]) == {
        "promo_full_file_het_cohen_new_source.csv",
        "het_cohen_new_source_store_file.json",
        f"{het_cohen}.json",
    }
    assert set(groups[mahsani]) == {
        "price_file_mahsani_ashuk_new_source.csv",
        "mahsani_ashuk_new_source_promo_file.json",
        f"{mahsani}.json",
    }
    assert groups[legacy_victory] == ["price_full_file_victory.csv"]
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


def test_pack_staged_files_keeps_quality_files_next_to_index(long_term_uploader):
    dataset = long_term_uploader.dataset_path

    _write(os.path.join(dataset, "index.json"), '{"0":"2026-01-01"}')
    _write(os.path.join(dataset, "scraper_quality.json"), '{"iterations":[]}')
    _write(os.path.join(dataset, "parser_quality.json"), '{"iterations":[]}')
    _write(os.path.join(dataset, "pipeline_health.json"), '{"overall_healthy":true}')
    _write(os.path.join(dataset, "bareket.json"))

    long_term_uploader.pack_staged_files()

    remaining = sorted(os.listdir(dataset))
    assert remaining == sorted(
        [
            "index.json",
            "scraper_quality.json",
            "parser_quality.json",
            "pipeline_health.json",
            "bareket.zip",
        ]
    )


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
