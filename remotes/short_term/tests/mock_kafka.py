from unittest.mock import patch, MagicMock
import functools


def mock_kafka_db(func):
    """
    Decorator that mocks Kafka dependencies and provides in-memory Kafka simulation.
    Every test starts with no topics and simulates Kafka behavior without network calls.
    """

    @functools.wraps(func)
    def wrapper(self):
        # In-memory storage for topics and messages
        topics_data = {}  # topic_name -> list of messages

        # Mock producer that stores messages in memory
        mock_producer = MagicMock()

        def mock_send_and_wait(topic, key, value):
            if topic not in topics_data:
                topics_data[topic] = []
            topics_data[topic].append(value)
            return None

        mock_producer.send_and_wait = mock_send_and_wait

        # Mock admin client that manages topics list
        mock_admin = MagicMock()

        def mock_list_topics():
            return list(topics_data.keys())

        def mock_delete_topics(topics_to_delete):
            if isinstance(topics_to_delete, list):
                for topic in topics_to_delete:
                    topics_data.pop(topic, None)
            else:
                topics_data.pop(topics_to_delete, None)

        mock_admin.list_topics = mock_list_topics
        mock_admin.delete_topics = mock_delete_topics

        # Mock consumer for reading messages
        mock_consumer = MagicMock()

        async def mock_getmany(timeout_ms=None, max_records=None):
            # Return empty dict since we're not testing actual message consumption
            return {}

        async def mock_start():
            pass

        async def mock_stop():
            pass

        mock_consumer.getmany = mock_getmany
        mock_consumer.start = mock_start
        mock_consumer.stop = mock_stop

        # Patch all Kafka classes
        with patch(
            "remotes.short_term.kafka_db.AIOKafkaProducer", return_value=mock_producer
        ), patch(
            "remotes.short_term.kafka_db.AIOKafkaConsumer", return_value=mock_consumer
        ), patch(
            "remotes.short_term.kafka_db.KafkaAdminClient", return_value=mock_admin
        ):

            # Set up the uploader to use mocked components
            self.uploader.producer = mock_producer
            self.uploader.admin_client = mock_admin
            self.uploader._connection_tested = True
            self.uploader._loop = MagicMock()  # Mock the event loop

            # Override get_destinations_content to return stored messages
            def mock_get_destinations_content(table_name, filter=None):
                if filter is not None:
                    raise NotImplementedError(
                        "Filtering is not supported in Kafka implementation"
                    )

                topic_name = self.uploader._get_topic_name(table_name)
                messages = topics_data.get(topic_name, [])
                # Filter out warmup messages
                return [
                    msg
                    for msg in messages
                    if not (isinstance(msg, dict) and msg.get("warmup") == "true")
                ]

            # Override clean method to clear our in-memory storage
            def mock_clean_all_destinations():
                topics_data.clear()

            self.uploader.get_destinations_content = mock_get_destinations_content
            self.uploader._clean_all_destinations = mock_clean_all_destinations

            # Run the actual test
            return func(self)

    return wrapper
