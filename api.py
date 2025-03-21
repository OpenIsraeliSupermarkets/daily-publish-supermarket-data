from fastapi import FastAPI, HTTPException, Header, Request, Security
from starlette.middleware.base import BaseHTTPMiddleware
from access_layer import AccessLayer
from remotes import MongoDbUploader, KaggleUploader
from typing import Optional
from token_validator import TokenValidator, SupabaseTelemetry
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, Response
import time

from datetime import datetime, timedelta
from utils import get_long_term_database_connector, get_short_term_database_connector
from response_models import (
    ScrapedFiles,
    TypeOfFileScraped,
    AvailableChains,
    FileContent,
    ServiceHealth,
    LongTermDatabaseHealth,
    ShortTermDatabaseHealth,
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, Response
import time
from datetime import datetime, timedelta
from utils import get_long_term_database_connector, get_short_term_database_connector



class TelemetryMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self.telemetry = SupabaseTelemetry()

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time

        # Get response body
        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk

        # הכנת נתוני הטלמטריה
        telemetry_data = {
            "timestamp": datetime.now().isoformat(),
            "method": request.method,
            "path": request.url.path,
            "query_params": str(request.query_params),
            "process_time_ms": round(process_time * 1000, 2),
            "response_status": response.status_code,
            "response_size_bytes": len(response_body),
            "client_ip": request.client.host if request.client else None,
            "user_agent": request.headers.get("user-agent"),
            "authorization_method": str(
                request.headers.get("Authorization", "").replace("Bearer ", "")
            ),
        }

        # שליחת הנתונים ל-Supabase
        await self.telemetry.send_telemetry(telemetry_data)

        # Reconstruct response with the original body
        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )


class AuthMiddleware(BaseHTTPMiddleware):
    token_validator = TokenValidator()

    async def dispatch(self, request: Request, call_next):
        try:
            if request.url.path == "/docs" or request.url.path == "/openapi.json":
                response = await call_next(request)
                return response

            token = request.headers.get("Authorization")
            if not token:
                return JSONResponse(
                    status_code=401, content={"detail": "Missing Authorization header"}
                )

            if not token.startswith("Bearer "):
                return JSONResponse(
                    status_code=401, content={"detail": "Invalid token format"}
                )

            token = token.replace("Bearer ", "")
            if not self.token_validator.validate_token(token):
                return JSONResponse(
                    status_code=401, content={"detail": "Invalid token"}
                )

            response = await call_next(request)
            return response

        except Exception as e:
            return JSONResponse(status_code=500, content={"detail": str(e)})


security = HTTPBearer()
app = FastAPI(
    title="Supermarket API",
    description="API Documentation",
    version="1.0.0",
    openapi_tags=[
        {"name": "API", "description": "Main API endpoints"},
        {"name": "Health", "description": "Health check endpoints"}
    ],
)
app.add_middleware(AuthMiddleware)
app.add_middleware(TelemetryMiddleware)

# Initialize the access layer and token validator
access_layer = AccessLayer(
    short_term_database_connector=get_short_term_database_connector(),
    long_term_database_connector=get_long_term_database_connector(),
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
async def service_health_check(credentials: HTTPAuthorizationCredentials = Security(security)) -> ServiceHealth:
    return ServiceHealth(status="healthy", timestamp=datetime.now().astimezone().isoformat())


@app.get("/short_term_health", tags=["Health"])
async def is_short_term_updated(credentials: HTTPAuthorizationCredentials = Security(security)) -> ShortTermDatabaseHealth:
    return access_layer.is_short_term_updated()
     


@app.get("/long_term_health", tags=["Health"])
async def is_long_term_updated(credentials: HTTPAuthorizationCredentials = Security(security)) -> LongTermDatabaseHealth:
    return access_layer.is_long_term_updated()

