"""Module providing token validation functionality with Supabase."""

from utils import Logger
import os

from supabase import create_client, Client


class TokenValidator:  # pylint: disable=too-few-public-methods
    """Class for validating API tokens against Supabase database."""

    def __init__(self):
        """Initialize the TokenValidator with Supabase client."""
        supabase_url = os.getenv("SUPABASE_URL", None)
        supabase_key = os.getenv("SUPABASE_KEY", None)

        # For testing, if environment variables are not properly set, create a mock client
        if supabase_url is None or supabase_key is None:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY environment variables must be set"
            )
        else:
            self.supabase = create_client(supabase_url, supabase_key)

    def validate_token(self, token: str) -> bool:
        """
        Validate if a token exists and is active.

        Args:
            token: The token string to validate

        Returns:
            bool: True if token is valid, False otherwise
        """

        try:
            # Check if token exists and is active via direct SQL query
            result = self.supabase.rpc(
                "validate_token", {"input_token": token}
            ).execute()

            print(result)
            if len(result.data) == 0:
                return False

            # Update last used time
            token_id = result.data[0]["id"]
            self.supabase.rpc(
                "update_token_last_used", {"token_id": token_id}
            ).execute()

            return True

        except Exception as e:  # pylint: disable=broad-except
            # We need to catch any exception here to prevent API crashes during validation
            print("Error validating token: %s", str(e))
            return False


class SupabaseTelemetry:  # pylint: disable=too-few-public-methods
    """Class for sending telemetry data to Supabase."""

    def __init__(self):
        """Initialize the SupabaseTelemetry with Supabase client."""
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        if not supabase_url or not supabase_key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY environment variables must be set"
            )
        self.supabase: Client = create_client(supabase_url, supabase_key)

    async def send_telemetry(self, telemetry_data: dict):
        """
        Send telemetry data to Supabase.

        Args:
            telemetry_data: Dictionary containing telemetry data to send
        """
        try:
            # Send data to api_telemetry table in Supabase
            self.supabase.table("api_telemetry").insert(telemetry_data).execute()
        except Exception as e:  # pylint: disable=broad-except
            # We need to catch any exception here to prevent API crashes during telemetry
            Logger.error("Failed to send telemetry to Supabase: %s", e)
            # Continue logging even if Supabase send fails
            Logger.info("API Telemetry: %s", telemetry_data)
