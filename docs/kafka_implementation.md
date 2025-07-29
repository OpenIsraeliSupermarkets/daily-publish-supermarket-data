# Kafka Database Uploader

The Kafka implementation provides a way to send supermarket data messages to Kafka topics. It implements the `ShortTermDatabaseUploader` interface, making it compatible with the existing system architecture.

## Features

- **Async Support**: Full async/await support for high-performance message sending
- **Topic Management**: Automatic topic creation and management
- **Batch Operations**: Support for sending multiple messages efficiently
- **Error Handling**: Robust error handling with retry mechanisms
- **Context Manager**: Async context manager for easy resource management

## Installation

Add the Kafka dependency to your requirements:

```bash
pip install aiokafka==0.9.0
```

## Configuration

Set the Kafka bootstrap servers via environment variable or constructor:

```python
# Environment variable
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Or pass directly to constructor
uploader = KafkaDbUploader("localhost:9092")
```

## Usage

### Basic Usage

```python
from remotes.short_term.kafka_db import KafkaDbUploader

# Initialize uploader
uploader = KafkaDbUploader("localhost:9092")

# Send a single message
message = {
    "chainid": "bareket",
    "product_name": "Milk",
    "price": 5.99,
    "timestamp": "2024-01-01T10:00:00"
}

await uploader.send_message(
    chain="bareket",
    file_type="price",
    message=message
)
```

### Batch Messages

```python
messages = [
    {"product_name": "Milk", "price": 5.99},
    {"product_name": "Bread", "price": 3.50},
    {"product_name": "Eggs", "price": 12.99}
]

await uploader.send_batch_messages(
    chain="bareket",
    file_type="price",
    messages=messages
)
```

### Using Context Manager

```python
async with KafkaDbUploader("localhost:9092") as producer:
    await producer.send_message("bareket", "price", message)
    # Connection automatically closed when exiting context
```

### Standard Uploader Interface

The Kafka implementation also supports the standard uploader interface:

```python
uploader = KafkaDbUploader("localhost:9092")

# Create topic (auto-created on first message)
uploader._create_table("_id", "price_data")

# Insert data
items = [
    {"_id": "1", "product_name": "Milk", "price": 5.99},
    {"_id": "2", "product_name": "Bread", "price": 3.50}
]

uploader._insert_to_database("price_data", items)
```

## Topic Naming

Topics are automatically created with the following naming convention:

- **Direct messages**: `{file_type.lower()}-{chain}` (e.g., `price-bareket`)
- **Standard interface**: `supermarket_data_{table_name}` (e.g., `supermarket_data_price_data`)

## Message Format

Messages are automatically serialized as JSON and include:

- **Key**: Chain name or custom key for partitioning
- **Value**: The actual message data as JSON
- **Timestamp**: Automatically added to messages

## Error Handling

The implementation includes comprehensive error handling:

- **Connection failures**: Graceful handling with test mode support
- **Message send failures**: Individual retry for failed messages
- **Batch failures**: Fallback to individual message sending

## Testing

Run the Kafka tests:

```bash
pytest remotes/short_term/tests/test_kafka_db.py -v
```

## Example

See `examples/kafka_example.py` for complete usage examples.

## Limitations

- **Content Retrieval**: Kafka doesn't support direct content retrieval (requires consumer)
- **Topic Listing**: Requires admin client for topic listing (not implemented)
- **Update Checking**: Simplified implementation for activity checking

## Dependencies

- `aiokafka==0.9.0`: Async Kafka client
- `asyncio`: Python async support
- `json`: Message serialization

## Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default: `localhost:9092`) 