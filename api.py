from fastapi import FastAPI, HTTPException, Header, Request, Security
from starlette.middleware.base import BaseHTTPMiddleware
from access.access_layer import AccessLayer
from typing import Optional
from access.token_validator import TokenValidator, SupabaseTelemetry
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import ORJSONResponse, Response
import os

from datetime import datetime
from remotes import KaggleUploader, MongoDbUploader

from data_models.response import (
    ScrapedFiles,
    TypeOfFileScraped,
    AvailableChains,
    FileContent,
    PaginatedFileContent,
    ServiceHealth,
    LongTermDatabaseHealth,
    ShortTermDatabaseHealth,
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime
from access.middleware import AuthMiddleware, TelemetryMiddleware


security = HTTPBearer()
app = FastAPI(
    title="Supermarket API",
    description="API Documentation",
    version="1.0.0",
    openapi_tags=[
        {"name": "API", "description": "Main API endpoints"},
        {"name": "Health", "description": "Health check endpoints"},
    ],
    default_response_class=ORJSONResponse,
)
app.add_middleware(AuthMiddleware)
app.add_middleware(TelemetryMiddleware)


# Initialize the access layer and token validator
_access_layer_instance = None


def get_access_layer():
    """Singleton pattern for access layer to avoid connection issues during import."""
    global _access_layer_instance
    if _access_layer_instance is None:
        _access_layer_instance = AccessLayer(
            short_term_database_connector=MongoDbUploader(
                mongodb_uri=os.environ["MONGODB_URI"]
            ),
            long_term_database_connector=KaggleUploader(
                dataset_path=os.environ["KAGGLE_DATASET_REMOTE_NAME"],
                dataset_remote_name=os.environ["KAGGLE_DATASET_REMOTE_NAME"],
                when=datetime.now(),
            ),
        )
    return _access_layer_instance


@app.get("/list_chains", tags=["API"])
async def list_chains(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> AvailableChains:
    return get_access_layer().list_all_available_chains()


@app.get("/list_file_types", tags=["API"])
async def list_file_types(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> TypeOfFileScraped:
    return get_access_layer().list_all_available_file_types()


@app.get("/list_scraped_files", tags=["API"])
async def read_files(
    chain: str,
    file_type: Optional[str] = None,
    store_number: Optional[str] = None,
    after_extracted_date: Optional[str] = None,
    only_latest: bool = False,
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> ScrapedFiles:
    try:
        # Parse the date string if provided
        parsed_date = None
        if after_extracted_date:
            try:
                parsed_date = datetime.fromisoformat(
                    after_extracted_date.replace("Z", "+00:00")
                )
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)",
                )

        return get_access_layer().list_files_with_filters(
            chain=chain,
            file_type=file_type,
            store_number=store_number,
            after_extracted_date=parsed_date,
            only_latest=only_latest,
        )
    except HTTPException:
        # Re-raise HTTPExceptions to preserve their status codes
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/raw/file_content", tags=["API"])
async def file_content(
    chain: str,
    file: str,
    chunk_size: int = 100,
    offset: int = 0,
    limit: Optional[int] = None,
    cursor: Optional[str] = None,
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> PaginatedFileContent:
    try:
        actual_limit = limit if limit is not None else chunk_size
        return get_access_layer().get_file_content_with_cursor_pagination(
            chain=chain, file=file, limit=actual_limit, cursor=cursor
        )
    except HTTPException:
        # Re-raise HTTPExceptions to preserve their status codes
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/service_health", tags=["Health"])
async def service_health_check(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> ServiceHealth:
    return ServiceHealth(
        status="healthy", timestamp=datetime.now().astimezone().isoformat()
    )


@app.get("/short_term_health", tags=["Health"])
async def is_short_term_updated(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> ShortTermDatabaseHealth:
    return get_access_layer().is_short_term_updated()


@app.get("/long_term_health", tags=["Health"])
async def is_long_term_updated(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> LongTermDatabaseHealth:
    return get_access_layer().is_long_term_updated()
