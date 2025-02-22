from fastapi import FastAPI, HTTPException, Header, Request, Security
from starlette.middleware.base import BaseHTTPMiddleware
from access_layer import AccessLayer
from remotes import MongoDbUploader
from typing import Optional
from token_validator import TokenValidator
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/docs" or request.url.path == "/openapi.json":
            return await call_next(request)
            
        token = request.headers.get("Authorization")
        if not token:
            raise HTTPException(status_code=401, detail="Missing Authorization header")
            
        if not token.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Invalid token format")
            
        token = token.replace("Bearer ", "")
        if not token_validator.validate_token(token):
            raise HTTPException(status_code=401, detail="Invalid token")
            
        return await call_next(request)


security = HTTPBearer()
app = FastAPI(
    title="Your API",
    description="API Documentation",
    version="1.0.0",
    openapi_tags=[{"name": "API", "description": "API endpoints"}]
)
app.add_middleware(AuthMiddleware)
access_layer = AccessLayer(MongoDbUploader)
token_validator = TokenValidator()


@app.get("/list_chains/v0")
async def list_chains(credentials: HTTPAuthorizationCredentials = Security(security)):
    return {"chains": access_layer.list_all_available_chains()}


@app.get("/list_file_types/v0")
async def list_file_types(credentials: HTTPAuthorizationCredentials = Security(security)):
    return {"file_types": access_layer.list_all_available_file_types()}


@app.get("/list_files/v0")
async def read_files(
    chain: str, 
    file_type: Optional[str] = None,
    credentials: HTTPAuthorizationCredentials = Security(security)
):
    try:
        return {"files": access_layer.list_files(chain=chain, file_type=file_type)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/file_content/v0")
async def file_content(
    chain: str, 
    file: str,
    credentials: HTTPAuthorizationCredentials = Security(security)
):
    try:
        return {"records": access_layer.get_file_content(chain=chain, file=file)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

