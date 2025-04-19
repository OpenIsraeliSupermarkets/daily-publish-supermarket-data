from fastapi import  Request
from starlette.middleware.base import BaseHTTPMiddleware

from access.token_validator import TokenValidator, SupabaseTelemetry
from fastapi.responses import JSONResponse, Response
import time
from datetime import datetime


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
