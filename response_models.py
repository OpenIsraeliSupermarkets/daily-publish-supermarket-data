from pydantic import BaseModel
from typing import Union

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
    row_content: dict[str, Union[str,int,float]]


class FileContent(BaseModel):
    rows: list[RawFileContent]

    def __init__(self, rows: list[dict[str,str]]) -> None:
        processed_rows = [
            RawFileContent(
                found_folder=row["found_folder"],
                file_name=row["file_name"],
                row_index=row["row_index"],
                row_content={k: v for k, v in row.items() if k not in ["found_folder", "file_name", "row_index"]},
            )
            for row in rows
        ]
        super().__init__(rows=processed_rows)
