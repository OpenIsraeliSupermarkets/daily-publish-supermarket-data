from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordBearer
from il_supermarket_scarper import ScraperFactory, FileTypesFilters
from remotes import DummyDocumentDbUploader

app = FastAPI()
database = DummyDocumentDbUploader("il-central-1")
# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@app.get("/list_files/v0")
async def read_files(chain: str):
    if not chain:
        raise HTTPException(status_code=400, detail="Chain parameter is required")

    scraper = ScraperFactory.get(chain)
    if not scraper:
        raise HTTPException(
            status_code=401,
            detail=f"chain {scraper} is not a valid chain {ScraperFactory.all_scrapers_name()}",
        )

    return {"files": database._get_all_files_by_chain(chain)}


@app.get("/file_content/v0")
async def file_content(chain: str, file: str):
    if not chain:
        raise HTTPException(status_code=400, detail="chain parameter is required")
    if not file:
        raise HTTPException(status_code=400, detail="file parameter is required")

    scraper = ScraperFactory.get(chain)
    if not scraper:
        raise HTTPException(
            status_code=401,
            detail=f"chain {scraper} is not a valid chain {ScraperFactory.all_scrapers_name()}",
        )

    file_type = FileTypesFilters.get_type_from_file(file)
    if not file_type:
        raise HTTPException(
            status_code=400, detail=f"file {file} doesn't follow the correct pattern."
        )

    table_name = f"{file_type.name.lower()}_{chain.lower()}"
    return {"records": database._get_content_of_file(table_name, file)}
