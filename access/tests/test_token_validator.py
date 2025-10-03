"""Tests for token_validator.py module."""

import os
from unittest.mock import patch, MagicMock

import pytest

from access.token_validator import TokenValidator, SupabaseTelemetry


class TestTokenValidator:
    """Test cases for TokenValidator class."""

    @pytest.fixture
    def mock_supabase(self):
        """Setup mock for Supabase client."""
        with patch("access.token_validator.create_client") as mock_create_client:
            mock_client = MagicMock()
            mock_create_client.return_value = mock_client
            yield mock_client

    @pytest.fixture
    def token_validator(self):
        """Create TokenValidator instance with mocked Supabase client."""
        with patch.dict(
            os.environ, {"SUPABASE_URL": "https://mock.url", "SUPABASE_KEY": "mock_key"}
        ):
            with patch("access.token_validator.create_client") as mock_create_client:
                mock_client = MagicMock()
                mock_create_client.return_value = mock_client
                validator = TokenValidator()
                validator.supabase = mock_client
                return validator

    def test_init(self):
        """Test TokenValidator initialization."""
        with patch.dict(
            os.environ, {"SUPABASE_URL": "test_url", "SUPABASE_KEY": "test_key"}
        ):
            with patch("access.token_validator.create_client") as mock_create_client:
                TokenValidator()
                mock_create_client.assert_called_once_with("test_url", "test_key")

    def test_validate_token_valid(
        self, token_validator, mock_supabase
    ):  # pylint: disable=unused-argument
        """Test validating a valid token."""
        # Setup mock response
        mock_result = MagicMock()
        mock_result.data = [{"id": "test_token_id"}]
        token_validator.supabase.rpc().execute.return_value = mock_result

        # Test validation
        result = token_validator.validate_token("valid_token")

        # Verify
        assert result is True
        token_validator.supabase.rpc.assert_any_call(
            "validate_token", {"input_token": "valid_token"}
        )
        token_validator.supabase.rpc.assert_any_call(
            "update_token_last_used", {"token_id": "test_token_id"}
        )

    def test_validate_token_invalid(
        self, token_validator, mock_supabase
    ):  # pylint: disable=unused-argument
        """Test validating an invalid token."""
        # Setup mock response
        mock_result = MagicMock()
        mock_result.data = []  # Empty data indicates invalid token
        token_validator.supabase.rpc().execute.return_value = mock_result

        # Test validation
        result = token_validator.validate_token("invalid_token")

        # Verify
        assert result is False
        # Verify the rpc was called with the right parameters rather than checking call count
        token_validator.supabase.rpc.assert_any_call(
            "validate_token", {"input_token": "invalid_token"}
        )

    def test_validate_token_exception(
        self, token_validator, mock_supabase
    ):  # pylint: disable=unused-argument
        """Test exception handling during token validation."""
        # Setup mock to raise exception
        token_validator.supabase.rpc.side_effect = Exception("Test exception")

        # Test validation
        result = token_validator.validate_token("some_token")

        # Verify
        assert result is False


class TestSupabaseTelemetry:
    """Test cases for SupabaseTelemetry class."""

    @pytest.fixture
    def mock_supabase(self):
        """Setup mock for Supabase client."""
        with patch("access.token_validator.create_client") as mock_create_client:
            mock_client = MagicMock()
            mock_create_client.return_value = mock_client
            yield mock_client

    @pytest.fixture
    def telemetry(self):
        """Create SupabaseTelemetry instance with mocked Supabase client."""
        with patch.dict(
            os.environ, {"SUPABASE_URL": "https://mock.url", "SUPABASE_KEY": "mock_key"}
        ):
            with patch("access.token_validator.create_client") as mock_create_client:
                mock_client = MagicMock()
                mock_create_client.return_value = mock_client
                telemetry_instance = SupabaseTelemetry()
                telemetry_instance.supabase = mock_client
                return telemetry_instance

    def test_init(self):
        """Test SupabaseTelemetry initialization."""
        with patch.dict(
            os.environ, {"SUPABASE_URL": "test_url", "SUPABASE_KEY": "test_key"}
        ):
            with patch("access.token_validator.create_client") as mock_create_client:
                SupabaseTelemetry()
                mock_create_client.assert_called_once_with("test_url", "test_key")

    def test_init_missing_env_vars(self):
        """Test SupabaseTelemetry initialization with missing environment variables."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as excinfo:
                SupabaseTelemetry()
            msg = "SUPABASE_URL and SUPABASE_KEY environment variables must be set"
            assert msg in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_send_telemetry_success(
        self, telemetry, mock_supabase
    ):  # pylint: disable=unused-argument
        """Test successful telemetry data transmission."""
        # Test data
        telemetry_data = {"endpoint": "/test", "method": "GET", "status_code": 200}

        # Call method
        await telemetry.send_telemetry(telemetry_data)

        # Verify
        telemetry.supabase.table.assert_called_once_with("api_telemetry")
        telemetry.supabase.table().insert.assert_called_once_with(telemetry_data)
        telemetry.supabase.table().insert().execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_telemetry_exception(
        self, telemetry, mock_supabase
    ):  # pylint: disable=unused-argument
        """Test exception handling during telemetry transmission."""
        # Setup mock to raise exception
        telemetry.supabase.table.side_effect = Exception("Test exception")

        # Test data
        telemetry_data = {"endpoint": "/test", "method": "GET", "status_code": 500}

        # Call method with logging capture
        with patch("access.token_validator.Logger") as mock_logging:
            await telemetry.send_telemetry(telemetry_data)

            # Verify error was logged
            mock_logging.error.assert_called_once()
            assert (
                "Failed to send telemetry to Supabase"
                in mock_logging.error.call_args[0][0]
            )

            # Verify fallback info logging
            mock_logging.info.assert_called_once()
            assert "API Telemetry" in mock_logging.info.call_args[0][0]
