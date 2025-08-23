"""
Unit tests for the SupermarketDataPublisher class in dag_simulator.py.
Focuses on testing the run method and its scheduling logic with proper mocking.
"""

import pytest
import time
import datetime
from unittest.mock import Mock, patch, MagicMock
from publishers.dag_simulator import SupermarketDataPublisher


class TestSupermarketDataPublisher:
    """Test class for SupermarketDataPublisher."""

    @pytest.fixture
    def mock_publisher(self):
        """Create a mock publisher instance for testing."""
        return SupermarketDataPublisher(
            long_term_db_target=Mock(),
            short_term_db_target=Mock(),
            number_of_scraping_processes=2,
            app_folder="test_app",
            data_folder="test_data",
            outputs_folder="test_outputs",
            status_folder="test_status",
        )

    @pytest.fixture
    def mock_operations(self):
        """Create mock operations for testing."""
        return ["scrape", "convert", "upload"]

    @pytest.fixture
    def mock_final_operations(self):
        """Create mock final operations for testing."""
        return ["cleanup", "notify"]

    @pytest.fixture
    def fixed_datetime(self):
        """Create a fixed datetime for consistent testing."""
        return datetime.datetime(2024, 1, 1, 0, 0, 0)

    def test_init_defaults(self):
        """Test publisher initialization with default parameters."""
        publisher = SupermarketDataPublisher()
        
        assert publisher.executed_jobs == 0
        assert publisher.last_execution_time is None

    def test_init_custom_params(self):
        """Test publisher initialization with custom parameters."""
        custom_long_term = Mock()
        custom_short_term = Mock()
        
        publisher = SupermarketDataPublisher(
            long_term_db_target=custom_long_term,
            short_term_db_target=custom_short_term,
            number_of_scraping_processes=8,
            app_folder="custom_app",
            enabled_scrapers=["scraper1", "scraper2"]
        )
        
        assert publisher.long_term_db_target == custom_long_term
        assert publisher.short_term_db_target == custom_short_term
        assert publisher.number_of_scraping_processes == 8
        assert publisher.app_folder == "custom_app"
        assert publisher.enabled_scrapers == ["scraper1", "scraper2"]

    @patch('publishers.dag_simulator.datetime.datetime')
    def test_run_basic_execution_no_final_operations(self, mock_datetime, mock_publisher, mock_operations, fixed_datetime):
        """Test basic execution flow without final operations."""
        # Setup mocks
        # Simulate time advancing by 1 hour every second in a separate thread

        current_time = [fixed_datetime]
        duration_of_execution = 1
        sleep_time = 1
        for i in range(25):
            # end 
            end_execution_time = current_time[-1] + datetime.timedelta(hours=duration_of_execution)

            current_time.extend([end_execution_time, end_execution_time + datetime.timedelta(seconds=sleep_time)])

        def get_time(): 
            return current_time.pop(0)
        
        with patch.object(mock_publisher, '_now', side_effect=get_time):
            with patch('publishers.dag_simulator.SupermarketDataPublisherInterface.run') as mock_super_run:
                mock_publisher.run(operations="a,b,c",
                                final_operations="d,e",
                                wait_time_seconds=sleep_time,
                                should_execute_final_operations="EOD",
                                should_stop_dag="ONCE")
                
                # Should execute operations once
                assert mock_super_run.call_count == 25


   