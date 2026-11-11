"""Indicators on what the scrapers downloaded."""

from managers.indicators.base import Indicator


class AttemptedDownloadSuccessRate(Indicator):
    """downloaded / (downloaded + failed), each file counted once a day."""

    name = "attempted_download_success_rate"
    failure_label = "download_success_rate"
    section = "scraper"
    threshold_key = "scraper_download_success_rate_min"
    env_var = "QUALITY_SCRAPER_MIN_DOWNLOAD_SUCCESS_RATE"
    default = 0.95
    op = "min"

    def section_value(self, section):
        return section.get("attempted_download_success_rate")


class ScrapersWithNoData(Indicator):
    """Scrapers that downloaded nothing in the whole status window."""

    name = "scrapers_with_no_data"
    section = "scraper"
    threshold_key = "scraper_no_data_scrapers_max"
    env_var = "QUALITY_SCRAPER_MAX_NO_DATA_SCRAPERS"
    default = 0.0
    op = "max"

    def section_value(self, section):
        return section.get("scrapers_with_no_data")
