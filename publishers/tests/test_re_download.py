"""_execute_scraping asks the scraper to re-download unparsed files."""

import json
import os
from unittest import mock

from il_supermarket_scarper import DumpFolderNames

from publishers.base_publisher import BaseSupermarketDataPublisher


def _write_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def _publisher_with_unparsed_file(tmp_path, re_download_unparsed):
    publisher = BaseSupermarketDataPublisher(
        app_folder=str(tmp_path),
        enabled_scrapers=["BAREKET"],
        enabled_file_types=["PRICE_FILE"],
        re_download_unparsed=re_download_unparsed,
    )
    stem = DumpFolderNames["BAREKET"].value.lower()
    _write_json(
        os.path.join(publisher.scraping_status_folder, f"{stem}.json"),
        {
            "verified_downloads": [
                {"file_name": "Price111.xml", "task_id": "t1"},
                {"file_name": "Price222.xml", "task_id": "t1"},
            ]
        },
    )
    _write_json(
        os.path.join(publisher.converting_status_folder, "bareket_price_file.json"),
        {"parsed_files": [{"file_name": "Price111.xml", "task_id": "p1"}]},
    )
    return publisher


def _start_kwargs(publisher):
    with mock.patch("publishers.base_publisher.ScarpingTask") as task_cls:
        publisher._execute_scraping()
    return task_cls.return_value.start.call_args.kwargs


def test_execute_scraping_passes_unparsed_files(tmp_path):
    publisher = _publisher_with_unparsed_file(tmp_path, re_download_unparsed=True)
    assert _start_kwargs(publisher)["re_download"] == {"BAREKET": ["Price222"]}


def test_execute_scraping_skips_re_download_when_flag_is_off(tmp_path):
    publisher = _publisher_with_unparsed_file(tmp_path, re_download_unparsed=False)
    assert _start_kwargs(publisher)["re_download"] is None
