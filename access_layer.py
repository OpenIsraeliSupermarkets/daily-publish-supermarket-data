import os
import re
from il_supermarket_scarper import ScraperFactory, FileTypesFilters
from remotes import DummyDocumentDbUploader, MongoDbUploader, KaggleUploader
from token_validator import TokenValidator
from response_models import ScrapedFile, TypeOfFileScraped, ScrapedFiles
from response_models import (
    ScrapedFiles,
    TypeOfFileScraped,
    AvailableChains,
    FileContent,
    ServiceHealth,
    LongTermDatabaseHealth,
    ShortTermDatabaseHealth,
)
from datetime import datetime, timedelta

class AccessLayer:

    def __init__(
        self,
        short_term_database_connector: MongoDbUploader,
        long_term_database_connector: KaggleUploader,
    ):
        self.short_term_database_connector = short_term_database_connector(
            "il-central-1"
        )
        self.long_term_database_connector = long_term_database_connector()

    def list_all_available_chains(self) -> AvailableChains:
        return AvailableChains(list_of_chains=ScraperFactory.all_scrapers_name())

    def list_all_available_file_types(self) -> TypeOfFileScraped:
        return TypeOfFileScraped(
        list_of_file_types=FileTypesFilters.__members__.keys()
        )

    def is_short_term_updated(self) -> bool:
        is_updated = self.short_term_database_connector.is_parser_updated()
        return ShortTermDatabaseHealth(is_updated=is_updated, last_update=datetime.now().astimezone().isoformat())


    def is_long_term_updated(self) -> bool:
        is_updated = self.long_term_database_connector.was_updated_in_last_24h()
        return LongTermDatabaseHealth(is_updated=is_updated, last_update=datetime.now().astimezone().isoformat())

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
        for doc in self.short_term_database_connector._get_content_of_file("ParserStatus",{"index": {"$regex": filter_condition}}):
            if "response" in doc and "files_to_process" in doc["response"]:
                files.extend(doc["response"]["files_to_process"])

        return ScrapedFiles(
            processed_files=list(map(
            lambda file: ScrapedFile(file_name=file),
            files,
        )))
        

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

        file_type = FileTypesFilters.get_type_from_file(file)
        if not file_type:
            raise Exception(
                f"file {file} doesn't follow the correct pattern.",
            )

        table_name = f"{file_type.name.lower()}_{chain.lower()}"
        return FileContent(rows=self.short_term_database_connector._get_content_of_file(table_name, file))


if __name__ == "__main__":
    import os
    os.environ["MONGODB_URI"] = "mongodb://192.168.1.129:27017"
    token_validator = TokenValidator()
    assert token_validator.validate_token(os.getenv("TOKEN"))

    api = AccessLayer(
        short_term_database_connector=MongoDbUploader,
        long_term_database_connector=KaggleUploader
    )
    files = api.list_files(chain="CITY_MARKET_SHOPS")
    for file in files.processed_files:
        content = api.get_file_content(
            chain="CITY_MARKET_SHOPS", file=file.file_name
        )
        if len(content.rows) == 0:
            print(f"file {file.file_name} is empty")
