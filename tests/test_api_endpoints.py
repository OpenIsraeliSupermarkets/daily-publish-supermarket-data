"""Tests for API endpoints with new functionality."""

import pytest
import os
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import datetime

# Set up environment variables for testing
os.environ["SUPABASE_URL"] = "https://test.supabase.co"
os.environ["SUPABASE_KEY"] = "test_key"
os.environ["MONGODB_URI"] = "mongodb://test:test@localhost:27017/test"
os.environ["KAGGLE_DATASET_REMOTE_NAME"] = "test-dataset"

from fastapi.testclient import TestClient
from fastapi import HTTPException

from api import app
from data_models.response import (
    AvailableChains,
    TypeOfFileScraped,
    ScrapedFiles,
    PaginatedFileContent,
    ServiceHealth,
    ShortTermDatabaseHealth,
    LongTermDatabaseHealth,
)


class TestAPIEndpoints:
    """Test cases for API endpoints."""

    def setup_method(self):
        """Set up test environment before each test method."""
        self.client = TestClient(app)
        # Mock the middleware's token validator to always return True
        with patch("access.middleware.AuthMiddleware.token_validator") as mock_validator:
            mock_validator.validate_token.return_value = True
            self.client.headers.update({"Authorization": "Bearer test_token"})

    def test_list_chains(self):
        """Test the /list_chains endpoint."""
        with patch("access.access_layer.ScraperFactory.all_scrapers_name", return_value=["shufersal", "rami_levy"]):
            response = self.client.get("/list_chains")
            assert response.status_code == 200
            data = response.json()
            assert "list_of_chains" in data
            assert data["list_of_chains"] == ["shufersal", "rami_levy"]

    def test_list_file_types(self):
        """Test the /list_file_types endpoint."""
        with patch("access.access_layer.AccessLayer.list_all_available_file_types") as mock_method:
            mock_method.return_value = TypeOfFileScraped(list_of_file_types=["PRICES", "STORES"])
            response = self.client.get("/list_file_types")
            assert response.status_code == 200
            data = response.json()
            assert "list_of_file_types" in data
            assert "PRICES" in data["list_of_file_types"]
            assert "STORES" in data["list_of_file_types"]

    def test_list_scraped_files_basic(self):
        """Test the /list_scraped_files endpoint with basic parameters."""
        with patch("access.access_layer.ScraperFactory.all_scrapers_name", return_value=["shufersal"]):
            with patch("access.access_layer.AccessLayer.list_files_with_filters") as mock_method:
                mock_method.return_value = ScrapedFiles(
                    processed_files=[{"file_name": "test_file.xml"}]
                )
                
                response = self.client.get("/list_scraped_files?chain=shufersal")
                assert response.status_code == 200
                data = response.json()
                assert "processed_files" in data
                assert len(data["processed_files"]) == 1
                assert data["processed_files"][0]["file_name"] == "test_file.xml"

    def test_list_scraped_files_with_filters(self):
        """Test the /list_scraped_files endpoint with additional filters."""
        with patch("access.access_layer.ScraperFactory.all_scrapers_name", return_value=["shufersal"]):
            with patch("access.access_layer.AccessLayer.list_files_with_filters") as mock_method:
                mock_method.return_value = ScrapedFiles(
                    processed_files=[{"file_name": "filtered_file.xml"}]
                )
                
                response = self.client.get(
                    "/list_scraped_files?chain=shufersal&file_type=PRICES&store_number=123&only_latest=true"
                )
                assert response.status_code == 200
                
                # Verify the method was called with correct parameters
                mock_method.assert_called_once()
                call_args = mock_method.call_args
                assert call_args[1]["chain"] == "shufersal"
                assert call_args[1]["file_type"] == "PRICES"
                assert call_args[1]["store_number"] == "123"
                assert call_args[1]["only_latest"] is True

    def test_list_scraped_files_with_date_filter(self):
        """Test the /list_scraped_files endpoint with date filter."""
        with patch("access.access_layer.ScraperFactory.all_scrapers_name", return_value=["shufersal"]):
            with patch("access.access_layer.AccessLayer.list_files_with_filters") as mock_method:
                mock_method.return_value = ScrapedFiles(
                    processed_files=[{"file_name": "recent_file.xml"}]
                )
                
                response = self.client.get(
                    "/list_scraped_files?chain=shufersal&after_extracted_date=2024-01-15T10:00:00"
                )
                assert response.status_code == 200
                
                # Verify the method was called with correct parameters
                mock_method.assert_called_once()
                call_args = mock_method.call_args
                assert call_args[1]["chain"] == "shufersal"
                assert isinstance(call_args[1]["after_extracted_date"], datetime)

    def test_list_scraped_files_invalid_date_format(self):
        """Test the /list_scraped_files endpoint with invalid date format."""
        # Reset the singleton to ensure fresh mocking
        import api
        api._access_layer_instance = None
        
        with patch("access.access_layer.ScraperFactory.all_scrapers_name", return_value=["shufersal"]):
            with patch("api.get_access_layer") as mock_get_access_layer:
                mock_access_layer = MagicMock()
                mock_access_layer.list_files_with_filters.side_effect = HTTPException(status_code=400, detail="Invalid date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)")
                mock_get_access_layer.return_value = mock_access_layer
                response = self.client.get(
                    "/list_scraped_files?chain=shufersal&after_extracted_date=invalid-date"
                )
                assert response.status_code == 400
                data = response.json()
                assert "Invalid date format" in data["detail"]

    def test_file_content_paginated(self):
        """Test the /raw/file_content endpoint with pagination."""
        with patch("api.get_access_layer") as mock_get_access_layer:
            mock_access_layer = MagicMock()
            mock_access_layer.get_file_content_with_cursor_pagination.return_value = PaginatedFileContent(
                rows=[],
                total_count=100,
                has_more=True,
                offset=0,
                chunk_size=50
            )
            mock_get_access_layer.return_value = mock_access_layer

            response = self.client.get(
                "/raw/file_content?chain=shufersal&file=test.xml&chunk_size=50&offset=0"
            )
            assert response.status_code == 200
            data = response.json()
            assert "rows" in data
            assert "total_count" in data
            assert "has_more" in data
            assert "offset" in data
            assert "chunk_size" in data
            assert data["total_count"] == 100
            assert data["has_more"] is True
            assert data["offset"] == 0
            assert data["chunk_size"] == 50

    def test_service_health(self):
        """Test the /service_health endpoint."""
        response = self.client.get("/service_health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "timestamp" in data
        assert data["status"] == "healthy"

    def test_short_term_health(self):
        """Test the /short_term_health endpoint."""
        with patch("access.access_layer.AccessLayer.is_short_term_updated") as mock_method:
            mock_method.return_value = ShortTermDatabaseHealth(
                is_updated=True,
                last_update="2024-01-15T10:00:00+00:00"
            )
            
            response = self.client.get("/short_term_health")
            assert response.status_code == 200
            data = response.json()
            assert "is_updated" in data
            assert "last_update" in data
            assert data["is_updated"] is True

    def test_long_term_health(self):
        """Test the /long_term_health endpoint."""
        with patch("access.access_layer.AccessLayer.is_long_term_updated") as mock_method:
            mock_method.return_value = LongTermDatabaseHealth(
                is_updated=False,
                last_update="2024-01-15T10:00:00+00:00"
            )
            
            response = self.client.get("/long_term_health")
            assert response.status_code == 200
            data = response.json()
            assert "is_updated" in data
            assert "last_update" in data
            assert data["is_updated"] is False

    def test_authentication_required(self):
        """Test that authentication is required for protected endpoints."""
        # Test without authentication
        client_no_auth = TestClient(app)
        response = client_no_auth.get("/list_chains")
        assert response.status_code == 401

    def test_invalid_token(self):
        """Test that invalid tokens are rejected."""
        client_invalid_token = TestClient(app)
        client_invalid_token.headers.update({"Authorization": "Bearer invalid_token"})
        
        with patch("access.middleware.AuthMiddleware.token_validator") as mock_validator:
            mock_validator.validate_token.return_value = False
            response = client_invalid_token.get("/list_chains")
            assert response.status_code == 401


if __name__ == "__main__":
    pytest.main([__file__]) 