"""Data models for the Israeli supermarket data processing system.

This package contains the Pydantic models used for data validation, serialization,
and storage throughout the supermarket data processing pipeline.

Modules:
    raw_schema: Contains models for internal data storage and processing
    response: Contains models for API responses and service health

The models in this package define the structure of:
- Supermarket data files and their contents
- Scraper status and execution logs
- API responses and health checks
- Database models for both short-term and long-term storage
"""

from .raw_schema import (
    CommonModel,
    ExecutionLog,
    Response,
    ParserStatus,
    ScraperStartedStatus,
    ScraperCollectedStatus,
    DownloadedStatus,
    ScraperDownloadedStatus,
    FolderSize,
    ScraperEstimatedSizeStatus,
    ScraperStatus,
    DataTable,
    create_dynamic_table_class,
    get_table_name,
    file_name_to_table,
    list_all_dynamic_tables,
)

from .response import (
    ScrapedFile,
    ScrapedFiles,
    TypeOfFileScraped,
    AvailableChains,
    RawFileContent,
    FileContent,
    PaginatedFileContent,
    ServiceHealth,
    LongTermDatabaseHealth,
    ShortTermDatabaseHealth,
)

__all__ = [
    # From raw_schema
    "CommonModel",
    "ExecutionLog",
    "Response",
    "ParserStatus",
    "ScraperStartedStatus",
    "ScraperCollectedStatus",
    "DownloadedStatus",
    "ScraperDownloadedStatus",
    "FolderSize",
    "ScraperEstimatedSizeStatus",
    "ScraperStatus",
    "DataTable",
    "create_dynamic_table_class",
    "get_table_name",
    "file_name_to_table",
    "list_all_dynamic_tables",
    # From response
    "ScrapedFile",
    "ScrapedFiles",
    "TypeOfFileScraped",
    "AvailableChains",
    "RawFileContent",
    "FileContent",
    "PaginatedFileContent",
    "ServiceHealth",
    "LongTermDatabaseHealth",
    "ShortTermDatabaseHealth",
]
