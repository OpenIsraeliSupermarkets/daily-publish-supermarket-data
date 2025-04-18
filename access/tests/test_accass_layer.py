from access.access_layer import AccessLayer
from remotes import MongoDbUploader, KaggleUploader


def test_access_layer():
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

