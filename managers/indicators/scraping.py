"""Indicators on what the scrapers downloaded."""

from typing import Any, Dict

from managers.indicators.base import Indicator


class AttemptedDownloadSuccessRate(Indicator):
    """downloaded / (downloaded + failed), each file counted once a day."""

    name = "attempted_download_success_rate"
    failure_label = "download_success_rate"
    section = "scraper"
    threshold_key = "scraper_download_success_rate_min"
    env_var = "QUALITY_SCRAPER_MIN_DOWNLOAD_SUCCESS_RATE"
    op = "min"

    def section_value(self, section):
        return section.get("attempted_download_success_rate")

    @staticmethod
    def compute(downloaded: int, failed: int) -> float:
        """Share of download attempts that succeeded (1.0 with no attempts)."""
        attempted = downloaded + failed
        return round(downloaded / attempted, 4) if attempted > 0 else 1.0


class ScrapersWithNoData(Indicator):
    """Scrapers that downloaded nothing in the whole status window."""

    name = "scrapers_with_no_data"
    section = "scraper"
    threshold_key = "scraper_no_data_scrapers_max"
    env_var = "QUALITY_SCRAPER_MAX_NO_DATA_SCRAPERS"
    op = "max"

    def section_value(self, section):
        return section.get("scrapers_with_no_data")

    @staticmethod
    def compute(
        downloaded_by_scraper: Dict[str, int], max_in_one_iteration: int = 0
    ) -> int:
        """Scrapers that downloaded nothing in the whole window.

        One empty run during the day does not count. Without per-scraper
        numbers, falls back to the worst single iteration.
        """
        if not downloaded_by_scraper:
            return max_in_one_iteration
        return sum(1 for downloaded in downloaded_by_scraper.values() if downloaded == 0)


def scraper_section(scraper_quality: Dict[str, Any]) -> Dict[str, Any]:
    """The scraper section of pipeline_health.json."""
    total_saw = 0
    total_downloaded = 0
    download_failed = 0
    skipped_by_limit = 0
    skipped_by_filter = 0
    max_no_data_scrapers = 0
    downloaded_by_scraper: Dict[str, int] = {}

    for iteration in scraper_quality.get("iterations", []):
        max_no_data_scrapers = max(
            max_no_data_scrapers, iteration.get("scrapers_with_no_data", 0)
        )
        for scraper, scraper_metrics in iteration.get("scrapers", {}).items():
            downloaded_by_scraper[scraper] = downloaded_by_scraper.get(
                scraper, 0
            ) + scraper_metrics.get("downloaded", 0)
            total_saw += scraper_metrics.get("saw", 0)
            total_downloaded += scraper_metrics.get("downloaded", 0)
            for entry in scraper_metrics.get("saw_not_downloaded", []):
                reason = entry.get("reason", "")
                if reason == "download_failed":
                    download_failed += 1
                elif reason == "skipped_by_limit":
                    skipped_by_limit += 1
                elif reason == "skipped_by_filter":
                    skipped_by_filter += 1

    download_success_rate = (
        total_downloaded / total_saw if total_saw > 0 else 1.0
    )

    return {
        "total_saw": total_saw,
        "total_downloaded": total_downloaded,
        "total_download_failed": download_failed,
        "total_skipped_by_limit": skipped_by_limit,
        "total_skipped_by_filter": skipped_by_filter,
        "scrapers_with_no_data": ScrapersWithNoData.compute(
            downloaded_by_scraper, max_no_data_scrapers
        ),
        "download_success_rate": round(download_success_rate, 4),
        "attempted_download_success_rate": AttemptedDownloadSuccessRate.compute(
            total_downloaded, download_failed
        ),
    }
