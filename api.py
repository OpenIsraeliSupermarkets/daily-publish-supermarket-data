from fastapi import FastAPI, HTTPException, Header, Request, Security
from starlette.middleware.base import BaseHTTPMiddleware
from access.access_layer import AccessLayer
from typing import Optional
from access.token_validator import TokenValidator, SupabaseTelemetry
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, Response
import os

from datetime import datetime
from remotes import KaggleUploader, MongoDbUploader

from data_models.response import (
    ScrapedFiles,
    TypeOfFileScraped,
    AvailableChains,
    FileContent,
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
)
app.add_middleware(AuthMiddleware)
app.add_middleware(TelemetryMiddleware)


# Initialize the access layer and token validator
access_layer = AccessLayer(
    short_term_database_connector=MongoDbUploader(
        mongodb_uri=os.environ["MONGODB_URI"]
    ),
    long_term_database_connector=KaggleUploader(
        dataset_path=os.environ["KAGGLE_DATASET_REMOTE_NAME"],
        dataset_remote_name=os.environ["KAGGLE_DATASET_REMOTE_NAME"],
        when=datetime.now(),
    ),
)


@app.get("/list_chains", tags=["API"])
async def list_chains(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> AvailableChains:
    return access_layer.list_all_available_chains()


@app.get("/list_file_types", tags=["API"])
async def list_file_types(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> TypeOfFileScraped:
    return access_layer.list_all_available_file_types()


@app.get("/list_scraped_files", tags=["API"])
async def read_files(
    chain: str,
    file_type: Optional[str] = None,
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> ScrapedFiles:
    try:
        return access_layer.list_files(chain=chain, file_type=file_type)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/raw/file_content", tags=["API"])
async def file_content(
    chain: str,
    file: str,
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> FileContent:
    try:
        return access_layer.get_file_content(chain=chain, file=file)
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
    return access_layer.is_short_term_updated()


@app.get("/long_term_health", tags=["Health"])
async def is_long_term_updated(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> LongTermDatabaseHealth:
    return access_layer.is_long_term_updated()
