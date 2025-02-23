from pydantic import BaseModel


class ScrapedFile(BaseModel):
    file_name: str


class ScrapedFiles(BaseModel):
    processed_files: list[ScrapedFile]


class TypeOfFileScraped(BaseModel):
    list_of_file_types: list[str]


class AvailableChains(BaseModel):
    list_of_chains: list[str]


class RawFileContent(BaseModel):
    row_index: str
    found_folder: str
    file_name: str
    row_content: dict[str, str]


class FileContent(BaseModel):
    rows: list[RawFileContent]

    def __init__(self, rows: list[dict[str, str]]):
        self.rows = map(
            lambda row: RawFileContent(
                row_index=row["row_index"],
                found_folder=row["found_folder"],
                file_name=row["file_name"],
                row_content=row,
            ),
            rows,
        )
