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


def test_execute_scraping_passes_unparsed_files(tmp_path):
    publisher = BaseSupermarketDataPublisher(
        app_folder=str(tmp_path),
        enabled_scrapers=["BAREKET"],
        enabled_file_types=["PRICE_FILE"],
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

    with mock.patch("publishers.base_publisher.ScarpingTask") as task_cls:
        publisher._execute_scraping()

    start_kwargs = task_cls.return_value.start.call_args.kwargs
    assert start_kwargs["file_to_download"] == {"BAREKET": ["Price222"]}
