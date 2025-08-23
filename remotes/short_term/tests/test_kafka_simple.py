# #!/usr/bin/env python3
# """
# Simple script to test Kafka functionality against a real Kafka instance.

# Assumes Kafka is already running at localhost:9092

# Usage:
#     python test_kafka_simple.py
# """

# import unittest
# import sys
# import os
# import copy

# # Add the current directory to Python path so we can import modules
# sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# from remotes.short_term.kafka_db import KafkaDbUploader


# class RealKafkaTestCase(unittest.TestCase):
#     """Test case for running against real Kafka without mocks."""
    
#     def setUp(self):
#         self.uploader = KafkaDbUploader(kafka_bootstrap_servers="localhost:9092")
#         # Clean up any existing topics before each test
#         try:
#             self.uploader._clean_all_destinations()
#         except Exception as e:
#             print(f"Warning: Could not clean up before test: {e}")
        
#     def tearDown(self):
#         # Clean up topics after each test (this won't actually delete them in real Kafka)
#         try:
#             self.uploader._clean_all_destinations()
#         except Exception as e:
#             print(f"Warning: Could not clean up after test: {e}")
        
#         # Properly disconnect from Kafka
#         try:
#             self.uploader.disconnect()
#         except Exception as e:
#             print(f"Warning: Could not disconnect from Kafka: {e}")

#     def test_create_and_insert_to_table(self):
#         import time
#         # Use unique table name to avoid conflicts
#         table_name = f"test_table_{int(time.time())}"
        
#         # Test table creation
#         self.uploader._create_destinations("id", table_name)
#         self.assertIn(table_name, self.uploader._list_destinations())

#         # Test data insertion
#         test_items = [{"id": 1, "data": "test1"}, {"id": 2, "data": "test2"}]
#         self.uploader._insert_to_destinations(
#             table_name, copy.deepcopy(test_items)
#         )
        
#         # Give some time for messages to be available
#         time.sleep(1)
        
#         result = list(self.uploader.get_destinations_content(table_name))
#         print(f"DEBUG: Got {len(result)} messages: {result}")
        
#         # Filter only actual data items (not warmup/flush messages)
#         data_items = [item for item in result if "id" in item and "data" in item]
        
#         self.assertEqual(
#             sorted(data_items, key=lambda x: x["id"]),
#             sorted(test_items, key=lambda x: x["id"]),
#         )

#     def test_clean_all_destinations(self):
#         # In real Kafka, topic deletion is disabled for safety
#         # So we test that the clean method runs without error
#         initial_topics = self.uploader._list_destinations()
        
#         # Create some test tables
#         self.uploader._create_destinations("id", "test_table1_clean")
#         self.uploader._create_destinations("id", "test_table2_clean")
        
#         # Call clean (which will log warnings but not actually delete)
#         self.uploader._clean_all_destinations()
        
#         # Since deletion is disabled, topics should still exist
#         final_topics = self.uploader._list_destinations()
#         self.assertGreaterEqual(len(final_topics), len(initial_topics))
        
#         print(f"✅ Clean destinations test passed. Topics remain for safety: {final_topics}")

#     def test_get_destinations_content(self):
#         import time
#         # Use unique table name
#         table_name = f"files_{int(time.time())}"
        
#         # Create test data
#         self.uploader._create_destinations("id", table_name)
#         test_items = [
#             {"id": "1", "chain": "chain1", "file_type": "csv", "data": "test1"},
#             {"id": "2", "chain": "chain1", "file_type": "json", "data": "test2"},
#             {"id": "3", "chain": "chain2", "file_type": "csv", "data": "test3"},
#         ]
#         self.uploader._insert_to_destinations(table_name, test_items)

#         # Give some time for messages to be available
#         time.sleep(1)

#         # Test reading the content we just inserted
#         files = self.uploader.get_destinations_content(table_name)
#         print(f"DEBUG: Got {len(files)} files: {files}")
        
#         # Filter only actual data items
#         data_items = [item for item in files if "id" in item and "chain" in item]
#         self.assertEqual(len(data_items), 3)

#     def test_collection_updated(self):
#         import time
#         # Use unique table name
#         table_name = f"update_test_{int(time.time())}"
        
#         # Test recent update
#         self.uploader._create_destinations("id", table_name)
        
#         # For real Kafka, topics with only warmup messages are considered "not updated"
#         # But they might still be considered updated, so let's test the actual functionality
#         initial_state = self.uploader._is_collection_updated(table_name)
#         print(f"DEBUG: Initial state for {table_name}: {initial_state}")

#         # Test with actual data
#         self.uploader._insert_to_destinations(
#             table_name, [{"id": "1", "data": "test"}]
#         )
        
#         # Give some time for the message to be processed
#         time.sleep(1)
        
#         updated_state = self.uploader._is_collection_updated(table_name)
#         print(f"DEBUG: Updated state for {table_name}: {updated_state}")
        
#         # After adding real data, it should be considered updated
#         self.assertTrue(updated_state)


# def main():
#     """Run the tests against real Kafka."""
#     print("🧪 Testing Kafka implementation against real Kafka instance...")
#     print("📍 Expected Kafka location: localhost:9092")
#     print("=" * 60)
    
#     # Test basic connectivity first
#     try:
#         uploader = KafkaDbUploader(kafka_bootstrap_servers="localhost:9092")
#         topics = uploader._list_destinations()
#         print(f"✅ Connected to Kafka. Found {len(topics)} existing topics.")
#     except Exception as e:
#         print(f"❌ Cannot connect to Kafka: {e}")
#         print("💡 Make sure Kafka is running at localhost:9092")
#         print("   You can start it with: docker-compose up -d zookeeper kafka")
#         return 1
    
#     # Run the actual tests
#     loader = unittest.TestLoader()
#     suite = loader.loadTestsFromTestCase(RealKafkaTestCase)
    
#     runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
#     result = runner.run(suite)
    
#     if result.wasSuccessful():
#         print("\n✅ All tests passed against real Kafka!")
#         return 0
#     else:
#         print(f"\n❌ Tests failed: {len(result.failures)} failures, {len(result.errors)} errors")
#         return 1


# if __name__ == "__main__":
#     sys.exit(main())
