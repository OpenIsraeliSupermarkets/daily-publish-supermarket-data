#!/usr/bin/env python3
"""Example usage of Kafka database uploader.

This script demonstrates how to use the KafkaDbUploader to send
supermarket data messages to Kafka topics.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any

from remotes.short_term.kafka_db import KafkaDbUploader


async def example_single_message():
    """Example of sending a single message to Kafka."""
    kafka_servers = "localhost:9092"
    
    async with KafkaDbUploader(kafka_bootstrap_servers=kafka_servers) as producer:
        # Example message for price data
        message = {
            "row_index": 1,
            "found_folder": "/data/csv/bareket",
            "file_name": "price_full.csv",
            "row_content": {
                "chainid": "bareket",
                "storeid": "001",
                "priceupdatedate": "2024-01-01 10:00:00",
                "product_name": "Milk",
                "price": 5.99
            },
            "content": {
                "chainid": "bareket",
                "storeid": "001",
                "priceupdatedate": "2024-01-01 10:00:00"
            },
            "timestamp": datetime.now().isoformat()
        }
        
        # Send message to price-full-bareket topic
        await producer.send_message(
            chain="bareket",
            file_type="price_full",
            message=message
        )
        
        print("Single message sent successfully!")


async def example_batch_messages():
    """Example of sending multiple messages to Kafka."""
    kafka_servers = "localhost:9092"
    
    async with KafkaDbUploader(kafka_bootstrap_servers=kafka_servers) as producer:
        # Example batch of messages
        messages = [
            {
                "row_index": 1,
                "product_name": "Milk",
                "price": 5.99,
                "chainid": "bareket",
                "storeid": "001",
                "timestamp": datetime.now().isoformat()
            },
            {
                "row_index": 2,
                "product_name": "Bread",
                "price": 3.50,
                "chainid": "bareket",
                "storeid": "001",
                "timestamp": datetime.now().isoformat()
            },
            {
                "row_index": 3,
                "product_name": "Eggs",
                "price": 12.99,
                "chainid": "bareket",
                "storeid": "001",
                "timestamp": datetime.now().isoformat()
            }
        ]
        
        # Send batch messages to price-bareket topic
        await producer.send_batch_messages(
            chain="bareket",
            file_type="price",
            messages=messages
        )
        
        print(f"Batch of {len(messages)} messages sent successfully!")


async def example_using_uploader_interface():
    """Example using the uploader interface (same as MongoDB)."""
    kafka_servers = "localhost:9092"
    
    uploader = KafkaDbUploader(kafka_bootstrap_servers=kafka_servers)
    
    # Create a table (topic)
    uploader._create_table("_id", "price_data")
    
    # Insert data using the standard interface
    items = [
        {
            "_id": "1",
            "chainid": "bareket",
            "product_name": "Milk",
            "price": 5.99,
            "timestamp": datetime.now().isoformat()
        },
        {
            "_id": "2",
            "chainid": "bareket",
            "product_name": "Bread",
            "price": 3.50,
            "timestamp": datetime.now().isoformat()
        }
    ]
    
    uploader._insert_to_database("price_data", items)
    
    # Check if collection was updated recently
    is_updated = uploader._is_collection_updated("price_data", seconds=3600)
    print(f"Collection updated recently: {is_updated}")
    
    # List tables (topics)
    tables = uploader._list_tables()
    print(f"Available tables: {tables}")
    
    print("Uploader interface example completed!")


def main():
    """Run all examples."""
    logging.basicConfig(level=logging.INFO)
    
    print("=== Kafka Database Uploader Examples ===\n")
    
    # Run examples
    asyncio.run(example_single_message())
    print()
    
    asyncio.run(example_batch_messages())
    print()
    
    asyncio.run(example_using_uploader_interface())
    print()
    
    print("All examples completed successfully!")


if __name__ == "__main__":
    main() 