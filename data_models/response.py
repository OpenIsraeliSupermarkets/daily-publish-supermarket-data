from pydantic import BaseModel
from typing import Union


class ScrapedFile(BaseModel):
    """Model representing a single scraped file.

    Stores the name of a file that has been processed by the scraper.
    """

    file_name: str


class ScrapedFiles(BaseModel):
    """Collection of scraped files.

    Contains a list of all files that have been processed during a scraping operation.
    """

    processed_files: list[ScrapedFile]


class TypeOfFileScraped(BaseModel):
    """Model for the types of files that were scraped.

    Stores a list of file type identifiers that have been scraped from supermarket websites.
    """

    list_of_file_types: list[str]


class AvailableChains(BaseModel):
    """Model for available supermarket chains.

    Contains a list of all supermarket chain identifiers that are available for scraping.
    """

    list_of_chains: list[str]


class RawFileContent(BaseModel):
    """Model for a single row of raw file content.

    Contains the metadata and content of a row extracted from a supermarket data file.
    """

    row_index: int
    found_folder: str
    file_name: str
    row_content: dict[str, Union[str, int, float]]


class FileContent(BaseModel):
    """Collection of raw file content rows.

    Contains all rows extracted from a supermarket data file, with processing
    applied during initialization.
    """

    rows: list[RawFileContent]

    def __init__(self, rows: list[dict[str, str]]) -> None:
        """Initialize the file content from raw row data.

        Transforms raw dictionary rows into RawFileContent objects.

        Args:
            rows (list[dict[str, str]]): List of raw row data dictionaries
        """
        processed_rows = [
            RawFileContent(
                found_folder=row["found_folder"],
                file_name=row["file_name"],
                row_index=row["row_index"],
                row_content=row["content"],
            )
            for row in rows
        ]
        super().__init__(rows=processed_rows)


class PaginatedFileContent(BaseModel):
    """Paginated file content with metadata.

    Contains file content rows with pagination information for efficient data retrieval.
    """

    rows: list[RawFileContent]
    total_count: int
    has_more: bool
    offset: int
    chunk_size: int

    def __init__(self, rows: list[dict[str, str]], total_count: int, has_more: bool, offset: int, chunk_size: int) -> None:
        """Initialize the paginated file content from raw row data.

        Transforms raw dictionary rows into RawFileContent objects with pagination metadata.

        Args:
            rows (list[dict[str, str]]): List of raw row data dictionaries
            total_count (int): Total number of records available
            has_more (bool): Whether there are more records available
            offset (int): Current offset for pagination
            chunk_size (int): Size of the current chunk
        """
        processed_rows = [
            RawFileContent(
                found_folder=row["found_folder"],
                file_name=row["file_name"],
                row_index=row["row_index"],
                row_content=row["content"],
            )
            for row in rows
        ]
        super().__init__(rows=processed_rows, total_count=total_count, has_more=has_more, offset=offset, chunk_size=chunk_size)


class ServiceHealth(BaseModel):
    """Model for service health status.

    Contains information about the current health and status of the supermarket data service.
    """

    status: str
    timestamp: str


class LongTermDatabaseHealth(BaseModel):
    """Model for long-term database health status.

    Contains information about the update status of the long-term storage database.
    """

    is_updated: bool
    last_update: Union[str, None]


class ShortTermDatabaseHealth(BaseModel):
    """Model for short-term database health status.

    Contains information about the update status of the short-term storage database.
    """

    is_updated: bool
    last_update: Union[str, None]
