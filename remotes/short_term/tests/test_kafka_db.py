"""Tests for Kafka database uploader."""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime

from ..kafka_db import KafkaDbUploader


class TestKafkaDbUploader:
    """Test cases for KafkaDbUploader."""

    def setup_method(self):
        """Set up test fixtures."""
        self.uploader = KafkaDbUploader("localhost:9092")

    def test_init_with_default_servers(self):
        """Test initialization with default bootstrap servers."""
        uploader = KafkaDbUploader()
        assert uploader.bootstrap_servers == "localhost:9092"

    def test_init_with_custom_servers(self):
        """Test initialization with custom bootstrap servers."""
        uploader = KafkaDbUploader("kafka:9092")
        assert uploader.bootstrap_servers == "kafka:9092"

    @patch('os.getenv')
    def test_init_with_env_variable(self, mock_getenv):
        """Test initialization with environment variable."""
        mock_getenv.return_value = "env-kafka:9092"
        uploader = KafkaDbUploader()
        assert uploader.bootstrap_servers == "env-kafka:9092"

    def test_get_topic_name(self):
        """Test topic name generation."""
        topic_name = self.uploader._get_topic_name("test_table")
        assert topic_name == "supermarket_data_test_table"

    @patch('asyncio.get_running_loop')
    @patch('asyncio.new_event_loop')
    @patch('asyncio.set_event_loop')
    def test_ensure_connection_no_loop(self, mock_set_loop, mock_new_loop, mock_get_loop):
        """Test connection establishment when no event loop is running."""
        mock_get_loop.side_effect = RuntimeError("No running event loop")
        mock_loop = Mock()
        mock_new_loop.return_value = mock_loop
        
        with patch.object(self.uploader, '_connect_to_kafka'):
            self.uploader._ensure_connection()
            
        mock_new_loop.assert_called_once()
        mock_set_loop.assert_called_once_with(mock_loop)

    @patch('asyncio.get_running_loop')
    def test_ensure_connection_with_loop(self, mock_get_loop):
        """Test connection establishment when event loop is running."""
        mock_loop = Mock()
        mock_get_loop.return_value = mock_loop
        
        with patch.object(self.uploader, '_connect_to_kafka'):
            self.uploader._ensure_connection()
            
        mock_loop.run_until_complete.assert_called_once()

    @patch('aiokafka.AIOKafkaProducer')
    def test_connect_to_kafka_success(self, mock_producer_class):
        """Test successful Kafka connection."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        
        asyncio.run(self.uploader._connect_to_kafka())
        
        mock_producer_class.assert_called_once()
        mock_producer.start.assert_called_once()

    @patch('aiokafka.AIOKafkaProducer')
    def test_connect_to_kafka_failure(self, mock_producer_class):
        """Test Kafka connection failure."""
        from aiokafka.errors import KafkaError
        mock_producer_class.side_effect = KafkaError("Connection failed")
        
        with pytest.raises(KafkaError):
            asyncio.run(self.uploader._connect_to_kafka())

    @patch('aiokafka.AIOKafkaProducer')
    def test_connect_to_kafka_test_mode(self, mock_producer_class):
        """Test Kafka connection in test mode."""
        from aiokafka.errors import KafkaError
        self.uploader.bootstrap_servers = "test:9092"
        mock_producer_class.side_effect = KafkaError("Connection failed")
        
        # Should not raise exception in test mode
        asyncio.run(self.uploader._connect_to_kafka())
        assert self.uploader._connection_tested

    def test_insert_to_database_empty_items(self):
        """Test inserting empty items list."""
        with patch.object(self.uploader, '_ensure_connection'):
            self.uploader._insert_to_database("test_topic", [])
            # Should not raise any exceptions

    @patch('asyncio.get_running_loop')
    def test_insert_to_database_success(self, mock_get_loop):
        """Test successful database insertion."""
        mock_loop = Mock()
        mock_get_loop.return_value = mock_loop
        
        items = [{"_id": "1", "data": "test1"}, {"_id": "2", "data": "test2"}]
        
        with patch.object(self.uploader, '_ensure_connection'):
            self.uploader.producer = AsyncMock()
            self.uploader._loop = mock_loop
            
            self.uploader._insert_to_database("test_topic", items)
            
            assert mock_loop.run_until_complete.call_count == 2

    def test_create_table(self):
        """Test table creation (Kafka topics are auto-created)."""
        with patch.object(self.uploader, '_ensure_connection'):
            self.uploader._create_table("partition_id", "test_table")
            # Should not raise any exceptions

    def test_clean_all_tables(self):
        """Test cleaning all tables (not implemented for Kafka)."""
        self.uploader._clean_all_tables()
        # Should not raise any exceptions

    def test_is_collection_updated(self):
        """Test collection update check."""
        with patch.object(self.uploader, '_ensure_connection'):
            result = self.uploader._is_collection_updated("test_collection")
            assert result is True  # Simplified implementation returns True

    def test_list_tables(self):
        """Test listing tables (not implemented for Kafka)."""
        with patch.object(self.uploader, '_ensure_connection'):
            result = self.uploader._list_tables()
            assert result == []

    def test_get_table_content(self):
        """Test getting table content (not implemented for Kafka)."""
        result = self.uploader.get_table_content("test_table")
        assert result == []

    @patch('aiokafka.AIOKafkaProducer')
    async def test_send_message_success(self, mock_producer_class):
        """Test successful message sending."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        self.uploader.producer = mock_producer
        
        message = {"data": "test"}
        await self.uploader.send_message("test_chain", "price", message)
        
        mock_producer.send_and_wait.assert_called_once()

    @patch('aiokafka.AIOKafkaProducer')
    async def test_send_batch_messages_success(self, mock_producer_class):
        """Test successful batch message sending."""
        mock_producer = AsyncMock()
        mock_producer_class.return_value = mock_producer
        self.uploader.producer = mock_producer
        
        messages = [{"data": "test1"}, {"data": "test2"}]
        await self.uploader.send_batch_messages("test_chain", "price", messages)
        
        assert mock_producer.send_and_wait.call_count == 2

    async def test_context_manager(self):
        """Test async context manager."""
        with patch.object(self.uploader, '_connect_to_kafka'), \
             patch.object(self.uploader, '_disconnect_from_kafka'):
            async with self.uploader as uploader:
                assert uploader is self.uploader 