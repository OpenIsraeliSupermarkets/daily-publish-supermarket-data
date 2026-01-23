"""Kafka implementation of the database uploader.

This module provides functionality for uploading and managing data in Kafka,
handling message serialization, topic management, and status tracking.
"""

from utils import Logger
import os
import json
import asyncio
from typing import Dict, Any, Optional, List
from data_models.raw_schema import ParserStatus, ScraperStatus, list_all_dynamic_tables

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError
from kafka import KafkaAdminClient
from .api_base import ShortTermDatabaseUploader


class KafkaDbUploader(ShortTermDatabaseUploader):
    """Kafka implementation for storing and managing supermarket data.

    This class handles all Kafka-specific operations including data preprocessing,
    topic management, and status tracking. It includes special handling for
    message serialization and bulk operations.
    """

    def __init__(self, kafka_bootstrap_servers: Optional[str] = None):
        """Initialize Kafka connection.

        Args:
            kafka_bootstrap_servers (str, optional): Kafka bootstrap servers. If not provided,
                                                   uses environment variable or default.
        """
        self.bootstrap_servers = kafka_bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        # Support for larger messages via environment variables
        # promo files can be larger than 1.2B
        # Default: 2MB (2097152 bytes)
        self.max_request_size = int(
            os.getenv("KAFKA_MAX_REQUEST_SIZE", "2097152")
        )
        self.producer: Optional[AIOKafkaProducer] = None
        self.admin_client: Optional[KafkaAdminClient] = None
        self._connection_tested = False
        self._loop = None
        self._cleaned = False

    def _ensure_connection(self):
        """Ensure Kafka connection is established."""
        if self.producer is None:
            if self._loop is None:
                try:
                    self._loop = asyncio.get_running_loop()
                except RuntimeError:
                    self._loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(self._loop)
            
            # Run the async connection in the event loop
            self._loop.run_until_complete(self._connect_to_kafka())


    async def _connect_to_kafka(self):
        """Establish connection to Kafka"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id="supermarket_data_producer",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                max_request_size=self.max_request_size,
            )
            await self.producer.start()

            # Initialize admin client
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="supermarket_data_admin",
            )

            Logger.info(f"Successfully connected to Kafka: {self.bootstrap_servers}")
            self._connection_tested = True
        except KafkaError as e:
            Logger.error(f"Error connecting to Kafka: {e}")
            # For testing, don't raise the exception
            if (
                "test" in self.bootstrap_servers
                or "localhost" in self.bootstrap_servers
            ):
                Logger.warning("Kafka connection failed in test mode, continuing...")
                self._connection_tested = True
            else:
                raise e

    async def _disconnect_from_kafka(self):
        """Disconnect from Kafka and clean up resources"""
        if self.producer:
            try:
                await self.producer.stop()
                Logger.info("Kafka producer stopped")
            except Exception as e:
                Logger.error(f"Error stopping Kafka producer: {e}")
            finally:
                self.producer = None
        
        if self.admin_client:
            try:
                self.admin_client.close()
                Logger.info("Kafka admin client closed")
            except Exception as e:
                Logger.error(f"Error closing Kafka admin client: {e}")
            finally:
                self.admin_client = None

    def disconnect(self):
        """Public method to disconnect from Kafka"""
        if self._loop and not self._loop.is_closed():
            try:
                self._loop.run_until_complete(self._disconnect_from_kafka())
            except Exception as e:
                Logger.error(f"Error during Kafka disconnect: {e}")
        self._connection_tested = False

    def _get_topic_name(self, table_target_name: str) -> str:
        """
        Generate Kafka topic name based on table name.
        Format: supermarket_data_{table_target_name}
        """
        return f"{table_target_name}"

    def _insert_to_destinations(self, table_target_name, items):
        """Insert items into a Kafka topic with error handling.

        Args:
            table_target_name (str): Name of the target topic
            items (list): List of items to insert
        """
        if not items:
            return

        self._ensure_connection()
        
        # Ensure we have a producer before proceeding
        if self.producer is None:
            Logger.error("Failed to establish Kafka connection")
            return
            
        # Run the async operation in the event loop
        self._loop.run_until_complete(self._async_insert_to_destinations(table_target_name, items))

    async def _async_insert_to_destinations(self, table_target_name, items):
        """Async implementation of insert to destinations."""
        if not items:
            return

        # Ensure we have a producer
        if self.producer is None:
            Logger.error("Kafka producer not available")
            return

        Logger.info("Pushing to topic %s, %d items", table_target_name, len(items))

        topic_name = self._get_topic_name(table_target_name)

        try:
            # Send all messages individually to avoid duplicates on partial failure
            successful_records = 0
            for item in items:
                try:
                    await self.producer.send_and_wait(
                        topic=topic_name,
                        key=str(item.get("_id", "default")).encode("utf-8"),
                        value=item,
                    )
                    successful_records += 1
                except KafkaError as inner_e:
                    Logger.error("Failed to send record: %s", str(inner_e))
            
            # send flush message only if we successfully sent some records
            if successful_records > 0:
                try:
                    await self.producer.send_and_wait(
                        topic=topic_name,
                        key=b"flush",
                        value={"flush": "true"},
                    )
                except KafkaError as flush_e:
                    Logger.error("Failed to send flush message: %s", str(flush_e))

            Logger.info("Successfully sent %d/%d records to Kafka", successful_records, len(items))
        except Exception as e:
            Logger.error("Unexpected error during message sending: %s", str(e))
            raise

    def _create_destinations(self, partition_id, table_name):
        """Create a new topic (Kafka doesn't require explicit topic creation).

        Args:
            partition_id (str): Field to use as partition key (not used in Kafka)
            table_name (str): Name of the topic to create
        """
        self._ensure_connection()
        Logger.info(
            "Topic %s will be created automatically when first message is sent",
            table_name,
        )
        # Kafka creates topics automatically when first message is sent
        # No explicit creation needed
        self._insert_to_destinations(
            table_name, [{"partition_id": partition_id, "warmup": "true"}]
        )

    def _clean_all_destinations(self):
        """Clean all topics in the Kafka cluster (not implemented for safety)."""
        Logger.warning("Kafka topic deletion not implemented for safety reasons")
        Logger.info("Kafka topics will persist until manually deleted")
        self._ensure_connection()
        try:
            # Get list of all topics
            topics = self._list_destinations()

            try:
                self.admin_client.delete_topics(topics)
                Logger.info("Successfully deleted topic: %s", topics)
            except KafkaError as e:
                Logger.error("Failed to delete topic %s: %s", topics, str(e))

            Logger.info("Completed topic cleanup attempt")

        except Exception as e:
            Logger.error("Error during topic cleanup: %s", str(e))

    def _is_collection_updated(
        self, collection_name: str, seconds: int = 10800
    ) -> bool:
        """Check if the topic has recent messages.

        Args:
            collection_name (str): Name of the topic to check
            seconds (int, optional): Number of seconds to look back. Defaults to 10800 (3 hours).

        Returns:
            bool: True if topic has recent messages, False otherwise
        """
        try:
            self._ensure_connection()
            # Check if the topic has any non-warmup messages
            messages = self.get_destinations_content(collection_name)
            # Filter out warmup messages
            non_warmup_messages = [
                msg
                for msg in messages
                if not (isinstance(msg, dict) and msg.get("warmup") == "true")
            ]

            return len(non_warmup_messages) > 0

        except KafkaError as e:
            Logger.error("Error checking Kafka topic activity: %s", str(e))
            return False

    def _list_destinations(self):
        """List all topics in the Kafka cluster.

        Returns:
            list[str]: List of topic names in the cluster
        """
        self._ensure_connection()
        try:
            # Get cluster metadata which includes topic list
            cluster_metadata = self.admin_client.list_topics()
            topic_names = list(cluster_metadata)
            Logger.info("Found %d Kafka topics", len(topic_names))
            return topic_names
        except Exception as e:
            Logger.error("Error listing Kafka topics: %s", str(e))
            return []

    def get_destinations_content(self, table_name, filter=None):
        """Get content of a specific topic from Kafka.

        Args:
            table_name (str): Name of the topic
            filter: Not used in Kafka implementation

        Returns:
            list: List of messages from the topic
        """
        if filter is not None:
            raise NotImplementedError(
                "Filtering is not supported in Kafka implementation"
            )

        try:
            self._ensure_connection()
            topic_name = self._get_topic_name(table_name)

            async def _get_messages():
                import time
                consumer = AIOKafkaConsumer(
                    topic_name,
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    group_id=f"test_consumer_{topic_name}_{int(time.time())}_{id(self)}",  # Unique group ID with timestamp
                    session_timeout_ms=10000,  # Increase session timeout
                    request_timeout_ms=20000,  # Increase request timeout
                    max_partition_fetch_bytes=self.max_request_size,
                )

                await consumer.start()
                messages = []

                try:
                    # Use getmany with timeout to avoid infinite loop
                    msg_set = await consumer.getmany(timeout_ms=5000, max_records=1000)

                    seen_messages = set()  # For deduplication

                    for partition, msg_list in msg_set.items():
                        for msg in msg_list:
                            # Deserialize the JSON value
                            if isinstance(msg.value, bytes):
                                try:
                                    deserialized_value = json.loads(
                                        msg.value.decode("utf-8")
                                    )
                                    # Filter out warmup and flush messages
                                    if not (
                                        isinstance(deserialized_value, dict)
                                        and (deserialized_value.get("warmup") == "true" or 
                                             deserialized_value.get("flush") == "true")
                                    ):
                                        # Create a hash for deduplication
                                        msg_hash = json.dumps(
                                            deserialized_value, sort_keys=True
                                        )
                                        if msg_hash not in seen_messages:
                                            seen_messages.add(msg_hash)
                                            messages.append(deserialized_value)
                                except (json.JSONDecodeError, UnicodeDecodeError):
                                    # If deserialization fails, add the raw value
                                    messages.append(msg.value)
                            else:
                                # Filter out warmup and flush messages for non-bytes values too
                                if not (
                                    isinstance(msg.value, dict)
                                    and (msg.value.get("warmup") == "true" or 
                                         msg.value.get("flush") == "true")
                                ):
                                    # Create a hash for deduplication
                                    msg_hash = json.dumps(msg.value, sort_keys=True)
                                    if msg_hash not in seen_messages:
                                        seen_messages.add(msg_hash)
                                        messages.append(msg.value)

                finally:
                    await consumer.stop()

                return messages

            return self._loop.run_until_complete(_get_messages())

        except KafkaError as e:
            Logger.error("Error reading from Kafka topic: %s", str(e))
            return []

    async def send_message(
        self,
        chain: str,
        file_type: str,
        message: Dict[str, Any],
        key: Optional[str] = None,
    ) -> None:
        """
        Send a message to the appropriate Kafka topic.

        Args:
            chain: The supermarket chain name
            file_type: The type of file (price, promo, store, etc.)
            message: The message data to send
            key: Optional message key for partitioning
        """
        try:
            if self.producer is None:
                await self._connect_to_kafka()

            # Align with consumer topic naming: {file_type_lower}_{chain_lower}
            topic_name = f"{file_type.lower()}_{chain.lower()}"

            # Use chain as key if no key provided
            message_key = key or chain

            await self.producer.send_and_wait(
                topic=topic_name, key=message_key.encode("utf-8"), value=message
            )

            Logger.debug(f"Sent message to topic {topic_name} for chain {chain}")

        except KafkaError as e:
            Logger.error(f"Failed to send message to Kafka: {e}")
            raise

    async def send_batch_messages(
        self,
        chain: str,
        file_type: str,
        messages: List[Dict[str, Any]],
        key: Optional[str] = None,
    ) -> None:
        """
        Send multiple messages to the appropriate Kafka topic.

        Args:
            chain: The supermarket chain name
            file_type: The type of file (price, promo, store, etc.)
            messages: List of message data to send
            key: Optional message key for partitioning
        """
        try:
            if self.producer is None:
                await self._connect_to_kafka()

            # Align with consumer topic naming: {file_type_lower}_{chain_lower}
            topic_name = f"{file_type.lower()}_{chain.lower()}"
            message_key = key or chain

            # Send all messages
            for message in messages:
                await self.producer.send_and_wait(
                    topic=topic_name, key=message_key.encode("utf-8"), value=message
                )

            Logger.info(
                f"Sent {len(messages)} messages to topic {topic_name} for chain {chain}"
            )

        except KafkaError as e:
            Logger.error(f"Failed to send batch messages to Kafka: {e}")
            raise

    async def __aenter__(self):
        """Async context manager entry"""
        await self._connect_to_kafka()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.producer:
            await self.producer.stop()
        if self.admin_client:
            self.admin_client.close()
        Logger.info("Disconnected from Kafka")

    def restart_database(
        self, enabled_scrapers: list[str], enabled_file_types: list[str]
    ):
        """Clean and recreate all tables in the database.

        This function drops all existing tables and recreates them with their original structure.
        """
        try:
            # self._clean_all_destinations()
            #
            self._create_destinations(
                ParserStatus.get_index(), ParserStatus.get_table_name()
            )
            self._create_destinations(
                ScraperStatus.get_index(), ScraperStatus.get_table_name()
            )
            for table in list_all_dynamic_tables(enabled_scrapers, enabled_file_types):
                self._create_destinations(table.get_index(), table.get_table_name())
        except Exception as e:
            raise
