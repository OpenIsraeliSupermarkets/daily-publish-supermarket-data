"""Module providing access layer to interface with supermarket data system."""

from datetime import datetime
import re
from typing import Optional

from il_supermarket_scarper import ScraperFactory, FileTypesFilters
from data_models.raw_schema import ParserStatus, DataTable, get_table_name
from data_models.response import (
    ScrapedFile,
    ScrapedFiles,
    TypeOfFileScraped,
    AvailableChains,
    FileContent,
    PaginatedFileContent,
    LongTermDatabaseHealth,
    ShortTermDatabaseHealth,
)
from remotes import LongTermDatabaseUploader, ShortTermDatabaseUploader


class AccessLayer:
    """
    Access layer responsible for interfacing with databases and providing
    access to supermarket chain data.
    """

    def __init__(
        self,
        short_term_database_connector: ShortTermDatabaseUploader,
        long_term_database_connector: LongTermDatabaseUploader,
    ):
        """
        Initialize the AccessLayer with database connectors.

        Args:
            short_term_database_connector: Connector for short-term database
            long_term_database_connector: Connector for long-term database
        """
        self.short_term_database_connector = short_term_database_connector
        self.long_term_database_connector = long_term_database_connector

    def list_all_available_chains(self) -> AvailableChains:
        """
        Get a list of all available supermarket chains.

        Returns:
            AvailableChains: Object containing all available chains
        """
        return AvailableChains(list_of_chains=ScraperFactory.all_scrapers_name())

    def list_all_available_file_types(self) -> TypeOfFileScraped:
        """
        Get a list of all available file types.

        Returns:
            TypeOfFileScraped: Object containing all available file types
        """
        return TypeOfFileScraped(list_of_file_types=FileTypesFilters.__members__.keys())

    def is_short_term_updated(self) -> ShortTermDatabaseHealth:
        """
        Check if the short-term database is updated.

        Returns:
            ShortTermDatabaseHealth: Object containing update status and timestamp
        """
        is_updated = self.short_term_database_connector._is_collection_updated(
            ParserStatus.get_table_name(), seconds=60 * 60
        )
        return ShortTermDatabaseHealth(
            is_updated=is_updated, last_update=datetime.now().astimezone().isoformat()
        )

    def is_long_term_updated(self) -> LongTermDatabaseHealth:
        """
        Check if the long-term database is updated.

        Returns:
            LongTermDatabaseHealth: Object containing update status and timestamp
        """
        is_updated = self.long_term_database_connector.was_updated_in_last(
            seconds=60 * 60 * 24
        )
        return LongTermDatabaseHealth(
            is_updated=is_updated, last_update=datetime.now().astimezone().isoformat()
        )

    def list_files(self, chain: str, file_type: str = None) -> ScrapedFiles:
        """
        List files for a specific chain and optional file type.

        Args:
            chain: Name of the supermarket chain
            file_type: Optional type of file to filter by

        Returns:
            ScrapedFiles: Object containing list of scraped files

        Raises:
            ValueError: If chain parameter is missing or invalid
            ValueError: If file_type parameter is invalid
        """
        if not chain:
            raise ValueError("'chain' parameter is required")

        if chain not in ScraperFactory.all_scrapers_name():
            valid_chains = ",".join(ScraperFactory.all_scrapers_name())
            raise ValueError(
                f"chain '{chain}' is not a valid chain, valid chains are: {valid_chains}"
            )

        if file_type is not None and file_type not in FileTypesFilters.__members__:
            valid_types = ",".join(FileTypesFilters.__members__.keys())
            raise ValueError(
                f"file_type '{file_type}' is not a valid file type, "
                f"valid file types are: {valid_types}"
            )

        filter_condition = f".*{re.escape(chain)}.*"
        if file_type is not None:
            filter_condition = f".*{re.escape(file_type)}.*{re.escape(chain)}.*"

        files = []
        # pylint: disable=protected-access
        for doc in self.short_term_database_connector.get_table_content(
            ParserStatus.get_table_name(), {"index": {"$regex": filter_condition}}
        ):
            if "response" in doc and "files_to_process" in doc["response"]:
                files.extend(doc["response"]["files_to_process"])

        return ScrapedFiles(
            processed_files=list(
                map(
                    lambda file: ScrapedFile(file_name=file),
                    files,
                )
            )
        )

    def list_files_with_filters(
        self, 
        chain: str, 
        file_type: str = None,
        store_number: Optional[str] = None,
        after_extracted_date: Optional[datetime] = None,
        only_latest: bool = False
    ) -> ScrapedFiles:
        """
        List files for a specific chain with enhanced filtering options.

        Args:
            chain: Name of the supermarket chain
            file_type: Optional type of file to filter by
            store_number: Optional store number to filter by
            after_extracted_date: Optional date to filter files after
            only_latest: Whether to return only the latest files

        Returns:
            ScrapedFiles: Object containing list of scraped files

        Raises:
            ValueError: If chain parameter is missing or invalid
            ValueError: If file_type parameter is invalid
        """
        if not chain:
            raise ValueError("'chain' parameter is required")

        if chain not in ScraperFactory.all_scrapers_name():
            valid_chains = ",".join(ScraperFactory.all_scrapers_name())
            raise ValueError(
                f"chain '{chain}' is not a valid chain, valid chains are: {valid_chains}"
            )

        if file_type is not None and file_type not in FileTypesFilters.__members__:
            valid_types = ",".join(FileTypesFilters.__members__.keys())
            raise ValueError(
                f"file_type '{file_type}' is not a valid file type, "
                f"valid file types are: {valid_types}"
            )

        # Build filter condition
        filter_condition = f".*{re.escape(chain)}.*"
        if file_type is not None:
            filter_condition = f".*{re.escape(file_type)}.*{re.escape(chain)}.*"

        files = []
        # pylint: disable=protected-access
        for doc in self.short_term_database_connector.get_table_content(
            ParserStatus.get_table_name(), {"index": {"$regex": filter_condition}}
        ):
            if "response" in doc and "files_to_process" in doc["response"]:
                doc_files = doc["response"]["files_to_process"]
                
                # Apply additional filters
                if store_number:
                    doc_files = [f for f in doc_files if store_number in f]
                
                if after_extracted_date:
                    # Filter by date if available in the document
                    if "timestamp" in doc:
                        doc_timestamp = datetime.fromisoformat(doc["timestamp"].replace('Z', '+00:00'))
                        # Ensure both datetimes are timezone-aware for comparison
                        if after_extracted_date.tzinfo is None:
                            # If after_extracted_date is naive, make it timezone-aware using UTC
                            from datetime import timezone
                            after_extracted_date = after_extracted_date.replace(tzinfo=timezone.utc)
                        if doc_timestamp < after_extracted_date:
                            continue
                
                files.extend(doc_files)

        # If only_latest is True, we could implement logic to return only the most recent files
        # For now, we'll return all files that match the filters
        if only_latest and files:
            # Simple implementation: take the last file (assuming files are ordered by date)
            files = [files[-1]]

        return ScrapedFiles(
            processed_files=list(
                map(
                    lambda file: ScrapedFile(file_name=file),
                    files,
                )
            )
        )

    def get_file_content(self, chain: str, file: str) -> FileContent:
        """
        Get content of a specific file from a specific chain.

        Args:
            chain: Name of the supermarket chain
            file: Name of the file to retrieve

        Returns:
            FileContent: Object containing the file content

        Raises:
            ValueError: If chain or file parameters are missing or invalid
        """
        if not chain:
            raise ValueError("chain parameter is required")
        if not file:
            raise ValueError("file parameter is required")

        scraper = ScraperFactory.get(chain)
        if not scraper:
            valid_chains = ScraperFactory.all_scrapers_name()
            raise ValueError(f"chain '{chain}' is not a valid chain {valid_chains}")

        file_type = FileTypesFilters.get_type_from_file(file.replace("NULL", ""))
        if not file_type:
            raise ValueError(f"file {file} doesn't follow the correct pattern.")

        table_name = get_table_name(file_type.name, chain)
        # pylint: disable=protected-access
        return FileContent(
            rows=self.short_term_database_connector.get_table_content(
                table_name, DataTable.by_file_name(file)
            )
        )

    def get_file_content_paginated(
        self, 
        chain: str, 
        file: str, 
        chunk_size: int = 100, 
        offset: int = 0
    ) -> PaginatedFileContent:
        """
        Get paginated content of a specific file from a specific chain.

        Args:
            chain: Name of the supermarket chain
            file: Name of the file to retrieve
            chunk_size: Number of records to return per page
            offset: Number of records to skip

        Returns:
            PaginatedFileContent: Object containing the file content with pagination metadata

        Raises:
            ValueError: If chain or file parameters are missing or invalid
        """
        if not chain:
            raise ValueError("chain parameter is required")
        if not file:
            raise ValueError("file parameter is required")

        scraper = ScraperFactory.get(chain)
        if not scraper:
            valid_chains = ScraperFactory.all_scrapers_name()
            raise ValueError(f"chain '{chain}' is not a valid chain {valid_chains}")

        file_type = FileTypesFilters.get_type_from_file(file.replace("NULL", ""))
        if not file_type:
            raise ValueError(f"file {file} doesn't follow the correct pattern.")

        table_name = get_table_name(file_type.name, chain)
        
        # Get all rows for the file
        all_rows = self.short_term_database_connector.get_table_content(
            table_name, DataTable.by_file_name(file)
        )
        
        total_count = len(all_rows)
        
        # Apply pagination
        paginated_rows = all_rows[offset:offset + chunk_size]
        has_more = offset + chunk_size < total_count
        
        # Generate cursors for cursor-based pagination
        next_cursor = None
        prev_cursor = None
        
        if has_more:
            next_cursor = str(offset + chunk_size)
        
        if offset > 0:
            prev_cursor = str(max(0, offset - chunk_size))
        
        return PaginatedFileContent(
            rows=paginated_rows,
            total_count=total_count,
            has_more=has_more,
            offset=offset,
            chunk_size=chunk_size,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor
        )

    def get_file_content_with_cursor_pagination(
        self, 
        chain: str, 
        file: str, 
        limit: int = 100, 
        cursor: Optional[str] = None
    ) -> PaginatedFileContent:
        """
        Get paginated content of a specific file from a specific chain using cursor-based pagination.

        Args:
            chain: Name of the supermarket chain
            file: Name of the file to retrieve
            limit: Number of records to return per page
            cursor: Optional cursor for pagination (offset as string)

        Returns:
            PaginatedFileContent: Object containing the file content with pagination metadata

        Raises:
            ValueError: If chain or file parameters are missing or invalid
        """
        if not chain:
            raise ValueError("chain parameter is required")
        if not file:
            raise ValueError("file parameter is required")

        scraper = ScraperFactory.get(chain)
        if not scraper:
            valid_chains = ScraperFactory.all_scrapers_name()
            raise ValueError(f"chain '{chain}' is not a valid chain {valid_chains}")

        file_type = FileTypesFilters.get_type_from_file(file.replace("NULL", ""))
        if not file_type:
            raise ValueError(f"file {file} doesn't follow the correct pattern.")

        table_name = get_table_name(file_type.name, chain)
        
        # Get all rows for the file
        all_rows = self.short_term_database_connector.get_table_content(
            table_name, DataTable.by_file_name(file)
        )
        
        total_count = len(all_rows)
        
        # Convert cursor to offset
        offset = 0
        if cursor:
            try:
                offset = int(cursor)
            except ValueError:
                raise ValueError("Invalid cursor format")
        
        # Apply pagination
        paginated_rows = all_rows[offset:offset + limit]
        has_more = offset + limit < total_count
        
        # Generate cursors for cursor-based pagination
        next_cursor = None
        prev_cursor = None
        
        if has_more:
            next_cursor = str(offset + limit)
        
        if offset > 0:
            prev_cursor = str(max(0, offset - limit))
        
        return PaginatedFileContent(
            rows=paginated_rows,
            total_count=total_count,
            has_more=has_more,
            offset=offset,
            chunk_size=limit,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor
        )
