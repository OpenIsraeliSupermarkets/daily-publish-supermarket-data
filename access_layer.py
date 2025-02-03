from typing import Annotated

from il_supermarket_scarper import ScraperFactory, FileTypesFilters
from remotes import DummyDocumentDbUploader


class API:

    database_connector = DummyDocumentDbUploader("il-central-1")

    def list_files(cls, chain: str, file_type: str = None):
        if not chain:
            raise Exception(status_code=400, detail="Chain parameter is required")

        if chain not in ScraperFactory.all_scrapers_name():
            raise Exception(
                detail=f"chain {chain} is not a valid chain {ScraperFactory.all_scrapers_name()}",
            )
        if file_type is not None and file_type not in FileTypesFilters.__members__:
            raise Exception(
                detail=f"file_type {file_type} is not a valid file type",
            )

        return cls.database_connector._get_all_files_by_chain(chain, file_type)

    def get_file_content(cls, chain: str, file: str):
        if not chain:
            raise Exception(detail="chain parameter is required")
        if not file:
            raise Exception(detail="file parameter is required")

        scraper = ScraperFactory.get(chain)
        if not scraper:
            raise Exception(
                detail=f"chain {scraper} is not a valid chain {ScraperFactory.all_scrapers_name()}",
            )

        file_type = FileTypesFilters.get_type_from_file(file)
        if not file_type:
            raise Exception(
                detail=f"file {file} doesn't follow the correct pattern.",
            )

        table_name = f"{file_type.name.lower()}_{chain.lower()}"
        return cls.database_connector._get_content_of_file(table_name, file)


if __name__ == "__main__":

    api = API()
    files = api.list_files(chain="SHEFA_BARCART_ASHEM")
    for file in files:
        content = api.get_file_content(chain="SHEFA_BARCART_ASHEM", file=file)
        print(len(content))
