import os
from il_supermarket_scarper import ScraperFactory, FileTypesFilters
from remotes import DummyDocumentDbUploader, MongoDbUploader, KaggleUploader
from token_validator import TokenValidator
from response_models import ScrapedFile, TypeOfFileScraped, ScrapedFiles


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

    def list_all_available_chains(self) -> list[str]:
        return ScraperFactory.all_scrapers_name()

    def list_all_available_file_types(self) -> list[str]:
        return FileTypesFilters.__members__.keys()

    def is_short_term_updated(self) -> bool:
        return self.short_term_database_connector.is_parser_updated()

    def is_long_term_updated(self) -> bool:
        return self.long_term_database_connector.was_updated_in_last_24h()

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

        return map(
            lambda file: ScrapedFile(file_name=file),
            self.short_term_database_connector.is_collection_updated(chain, file_type),
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

        file_type = FileTypesFilters.get_type_from_file(file)
        if not file_type:
            raise Exception(
                f"file {file} doesn't follow the correct pattern.",
            )

        table_name = f"{file_type.name.lower()}_{chain.lower()}"
        return self.short_term_database_connector.get_content_of_file(table_name, file)


if __name__ == "__main__":

    token_validator = TokenValidator()
    assert token_validator.validate_token(os.getenv("SUPABASE_TOKEN"))

    api = AccessLayer(MongoDbUploader)
    files = api.list_files(chain="FRESH_MARKET_AND_SUPER_DOSH")
    for file in files:
        content = api.get_file_content(
            chain="FRESH_MARKET_AND_SUPER_DOSH", file=file.file_name
        )
        print(len(content))
