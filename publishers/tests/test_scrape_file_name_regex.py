"""SCRAPE_FILE_NAME_REGEX restricts the scrape to the named files (#186)."""

from unittest import mock

import pytest

from publishers.base_publisher import BaseSupermarketDataPublisher
from publishers.dag_simulator import SupermarketDataPublisher


def _task_kwargs(publisher):
    with mock.patch("publishers.base_publisher.ScarpingTask") as task_cls:
        publisher._execute_scraping()
    return task_cls.call_args.kwargs


@pytest.mark.parametrize(
    "cls", [BaseSupermarketDataPublisher, SupermarketDataPublisher]
)
def test_regex_reaches_the_scraping_task(tmp_path, cls):
    publisher = cls(
        app_folder=str(tmp_path),
        enabled_scrapers=["BAREKET"],
        enabled_file_types=["PRICE_FILE"],
        file_name_regex=r"Price7290875100001\-001",
    )
    assert _task_kwargs(publisher)["file_name_regex"] == r"Price7290875100001\-001"


@pytest.mark.parametrize("value", [None, ""])
def test_unset_or_empty_means_no_filter(tmp_path, value):
    publisher = BaseSupermarketDataPublisher(
        app_folder=str(tmp_path),
        enabled_scrapers=["BAREKET"],
        enabled_file_types=["PRICE_FILE"],
        file_name_regex=value,
    )
    assert _task_kwargs(publisher)["file_name_regex"] is None
