"""Module providing authentication and telemetry middleware for the API."""

import time
from datetime import datetime

from fastapi import Request
from fastapi.responses import JSONResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware

from access.token_validator import TokenValidator, SupabaseTelemetry


class TelemetryMiddleware(BaseHTTPMiddleware):  # pylint: disable=too-few-public-methods
    """Middleware for tracking and logging API request telemetry."""

    def __init__(self, app):
        """
        Initialize the TelemetryMiddleware.

        Args:
            app: The FastAPI application
        """
        super().__init__(app)
        self.telemetry = SupabaseTelemetry()

    async def dispatch(self, request: Request, call_next):
        """
        Process the request, measure performance, and record telemetry.

        Args:
            request: The incoming request
            call_next: The next middleware or endpoint handler

        Returns:
            Response: The API response
        """
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time

        # Get response body
        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk

        # Prepare telemetry data
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

        # Send data to Supabase
        await self.telemetry.send_telemetry(telemetry_data)

        # Reconstruct response with the original body
        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )


class AuthMiddleware(BaseHTTPMiddleware):  # pylint: disable=too-few-public-methods
    """Middleware for API authentication using token validation."""

    def __init__(self, app):
        """Initialize the middleware with lazy token validator."""
        super().__init__(app)
        self._token_validator = None

    @property
    def token_validator(self):
        """Lazy initialization of token validator."""
        if self._token_validator is None:
            self._token_validator = TokenValidator()
        return self._token_validator

    # pylint: disable=too-many-return-statements
    async def dispatch(self, request: Request, call_next):
        """
        Process the request, validate authentication token if required.

        Args:
            request: The incoming request
            call_next: The next middleware or endpoint handler

        Returns:
            Response: The API response or an error response
        """
        try:
            # Allow documentation endpoints without authentication
            if request.url.path in ("/docs", "/openapi.json"):
                return await call_next(request)

            # Validate Authorization header
            token = request.headers.get("Authorization")
            if not token:
                return JSONResponse(
                    status_code=401, content={"detail": "Missing Authorization header"}
                )

            if not token.startswith("Bearer "):
                return JSONResponse(
                    status_code=401, content={"detail": "Invalid token format"}
                )

            # Validate token
            token = token.replace("Bearer ", "")
            if not self.token_validator.validate_token(token):
                return JSONResponse(
                    status_code=401, content={"detail": "Invalid token"}
                )

            # Process the request if authentication succeeds
            return await call_next(request)

        except ValueError:
            return JSONResponse(
                status_code=400, content={"detail": "Invalid request parameters"}
            )
        except ConnectionError:
            return JSONResponse(
                status_code=503,
                content={"detail": "Authentication service unavailable"},
            )
        except Exception as e:  # pylint: disable=broad-except
            # We need to catch all exceptions here to prevent API crashes
            return JSONResponse(status_code=500, content={"detail": str(e)})
