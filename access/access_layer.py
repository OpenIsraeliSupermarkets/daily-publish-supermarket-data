"""Module providing access layer to interface with supermarket data system."""

from datetime import datetime
import re

from il_supermarket_scarper import ScraperFactory, FileTypesFilters
from data_models.raw_schema import ParserStatus, DataTable, get_table_name
from data_models.response import (
    ScrapedFile,
    ScrapedFiles,
    TypeOfFileScraped,
    AvailableChains,
    FileContent,
    LongTermDatabaseHealth,
    ShortTermDatabaseHealth,
)
from remotes import MongoDbUploader, KaggleUploader


class AccessLayer:
    """
    Access layer responsible for interfacing with databases and providing
    access to supermarket chain data.
    """

    def __init__(
        self,
        short_term_database_connector: MongoDbUploader,
        long_term_database_connector: KaggleUploader,
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
        is_updated = self.short_term_database_connector.is_parser_updated()
        return ShortTermDatabaseHealth(
            is_updated=is_updated, last_update=datetime.now().astimezone().isoformat()
        )

    def is_long_term_updated(self) -> LongTermDatabaseHealth:
        """
        Check if the long-term database is updated.

        Returns:
            LongTermDatabaseHealth: Object containing update status and timestamp
        """
        is_updated = self.long_term_database_connector.was_updated_in_last_24h()
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
