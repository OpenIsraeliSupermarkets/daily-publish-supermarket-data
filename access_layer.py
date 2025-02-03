from il_supermarket_scarper import ScraperFactory, FileTypesFilters
from remotes import DummyDocumentDbUploader, DynamoDbUploader


class AccessLayer:

    def __init__(self, database_connector: DynamoDbUploader):
        self.database_connector = database_connector("il-central-1")

    def list_all_available_chains(self):
        return ScraperFactory.all_scrapers_name()

    def list_all_available_file_types(self):
        return list(FileTypesFilters.__members__.keys())

    def list_files(self, chain: str, file_type: str = None):
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

        return self.database_connector._get_all_files_by_chain(chain, file_type)

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
        return self.database_connector._get_content_of_file(table_name, file)


if __name__ == "__main__":

    api = AccessLayer()
    files = api.list_files(chain="SALACH_DABACH")
    for file in files:
        content = api.get_file_content(chain="SALACH_DABACH", file=file)
        print(len(content))
