import os
import re
from il_supermarket_scarper import ScraperFactory, FileTypesFilters
from remotes import DummyDocumentDbUploader, MongoDbUploader, KaggleUploader
from data_models.raw_schema import ParserStatus, DataTable
from data_models.response import ScrapedFile, TypeOfFileScraped, ScrapedFiles
from data_models.response import (
    ScrapedFiles,
    TypeOfFileScraped,
    AvailableChains,
    FileContent,
    LongTermDatabaseHealth,
    ShortTermDatabaseHealth,
)
from datetime import datetime, timedelta
from data_models.raw_schema import get_table_name


class AccessLayer:

    def __init__(
        self,
        short_term_database_connector: MongoDbUploader,
        long_term_database_connector: KaggleUploader,
    ):
        self.short_term_database_connector = short_term_database_connector
        self.long_term_database_connector = long_term_database_connector

    def list_all_available_chains(self) -> AvailableChains:
        return AvailableChains(list_of_chains=ScraperFactory.all_scrapers_name())

    def list_all_available_file_types(self) -> TypeOfFileScraped:
        return TypeOfFileScraped(list_of_file_types=FileTypesFilters.__members__.keys())

    def is_short_term_updated(self) -> bool:
        is_updated = self.short_term_database_connector.is_parser_updated()
        return ShortTermDatabaseHealth(
            is_updated=is_updated, last_update=datetime.now().astimezone().isoformat()
        )

    def is_long_term_updated(self) -> bool:
        is_updated = self.long_term_database_connector.was_updated_in_last_24h()
        return LongTermDatabaseHealth(
            is_updated=is_updated, last_update=datetime.now().astimezone().isoformat()
        )

    def list_files(self, chain: str, file_type: str = None) -> ScrapedFiles:
        if not chain:
            raise Exception("'chain' parameter is required")

        if chain not in ScraperFactory.all_scrapers_name():
            raise Exception(
                f"chain '{chain}' is not a valid chain, valid chains are: {','.join(ScraperFactory.all_scrapers_name())}",
            )
        if file_type is not None and file_type not in FileTypesFilters.__members__:
            raise Exception(
                f"file_type '{file_type}' is not a valid file type, valid file types are: {','.join(FileTypesFilters.__members__.keys())}",
            )

        filter_condition = f".*{re.escape(chain)}.*"
        if file_type is not None:
            filter_condition = f".*{re.escape(file_type)}.*{re.escape(chain)}.*"

        files = []
        for doc in self.short_term_database_connector._get_table_content(
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

    def get_file_content(self, chain: str, file: str):
        if not chain:
            raise Exception("chain parameter is required")
        if not file:
            raise Exception("file parameter is required")

        scraper = ScraperFactory.get(chain)
        if not scraper:
            raise Exception(
                f"chain {scraper} is not a valid chain {ScraperFactory.all_scrapers_name()}",
            )

        file_type = FileTypesFilters.get_type_from_file(file.replace("NULL", ""))
        if not file_type:
            raise Exception(
                f"file {file} doesn't follow the correct pattern.",
            )

        table_name = get_table_name(file_type.name, chain)
        return FileContent(
            rows=self.short_term_database_connector._get_table_content(
                table_name, DataTable.by_file_name(file)
            )
        )
