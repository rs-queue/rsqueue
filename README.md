# RSQueue

A high-performance, thread-safe message queue service written in Rust with HTTP API access. RSQueue provides reliable FIFO message processing with visibility timeouts, atomic operations, message deduplication, and persistent queue specifications.

## Features

- 🚀 **High Performance**: In-memory message storage with Rust's zero-cost abstractions
- 🔒 **Thread-Safe**: Fully atomic operations using RwLock for concurrent access
- ⏱️ **Visibility Timeout**: Messages become invisible to other consumers for a configurable duration
- 🔁 **Message Deduplication**: Optional content-based deduplication using SHA-256 hashing
- ⏳ **Message TTL (Time To Live)**: Optional message expiration - messages automatically deleted after specified time
- 📝 **Persistent Queue Specs**: Queue configurations survive server restarts
- 🔄 **Automatic Re-queueing**: Unprocessed messages automatically return to the queue
- 📦 **Batch Operations**: Enqueue multiple messages in a single request with individual TTL settings
- 🆔 **UUID Tracking**: Unique IDs for messages and receipt handles for secure deletion
- 🗑️ **Queue Management**: Create, delete, list, and purge queues via API

## Installation

### Prerequisites

- Rust 1.70+ (install from [rustup.rs](https://rustup.rs/))
- Cargo (comes with Rust)

### Build from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/rsqueue.git
cd rsqueue

# Build the project
cargo build --release

# Run the server
cargo run --release
```

The server will start on `http://0.0.0.0:3000`

## Quick Start

### 1. Create a Queue

```bash
# Basic queue without deduplication
curl -X POST http://localhost:3000/queues \
  -H "Content-Type: application/json" \
  -d '{
    "name": "task-queue",
    "visibility_timeout_seconds": 120
  }'

# Queue with deduplication enabled
curl -X POST http://localhost:3000/queues \
  -H "Content-Type: application/json" \
  -d '{
    "name": "dedup-queue",
    "visibility_timeout_seconds": 120,
    "enable_deduplication": true,
    "deduplication_window_seconds": 300
  }'
```": "task-queue",
    "visibility_timeout_seconds": 120
  }'
```

### 2. Send a Message

```bash
# Basic message
curl -X POST http://localhost:3000/queues/task-queue/messages \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Process order #12345"
  }'

# Message with TTL (expires in 5 minutes)
curl -X POST http://localhost:3000/queues/task-queue/messages \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Temporary task",
    "ttl_seconds": 300
  }'
```

### 3. Receive Messages

```bash
curl -X POST http://localhost:3000/queues/task-queue/messages/get \
  -H "Content-Type: application/json" \
  -d '{
    "count": 1
  }'
```

Response:
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "content": "Process order #12345",
    "created_at": "2024-01-15T10:30:00Z",
    "receipt_handle": "650e8400-e29b-41d4-a716-446655440001",
    "visible_after": "2024-01-15T10:32:00Z"
  }
]
```

### 4. Delete a Message

```bash
curl -X DELETE http://localhost:3000/queues/task-queue/messages/650e8400-e29b-41d4-a716-446655440001
```

## API Reference

### Queue Management

#### Create Queue
- **POST** `/queues`
- **Body**:
  ```json
  {
    "name": "string",
    "visibility_timeout_seconds": 120  // optional, defaults to 120
  }
  ```
- **Returns**: Queue specification

#### List All Queues
- **GET** `/queues`
- **Returns**: Array of queue information with sizes and settings

#### Delete Queue
- **DELETE** `/queues/{queue_name}`
- **Returns**: 204 No Content on success

#### Purge Queue
- **POST** `/queues/{queue_name}/purge`
- **Description**: Removes all messages from a queue
- **Returns**: 204 No Content

### Message Operations

#### Send Single Message
- **POST** `/queues/{queue_name}/messages`
- **Body**:
  ```json
  {
    "content": "string",
    "ttl_seconds": 300  // optional, message expires after this many seconds
  }
  ```
- **Returns**: `{"id": "uuid"}`

#### Send Batch Messages
- **POST** `/queues/{queue_name}/messages/batch`
- **Body**:
  ```json
  {
    "messages": [
      {"content": "message1", "ttl_seconds": 60},
      {"content": "message2", "ttl_seconds": null},
      {"content": "message3", "ttl_seconds": 300}
    ]
  }
  ```
- **Returns**:
  ```json
  {
    "results": [
      {"id": "uuid1", "error": null},
      {"id": "uuid2", "error": null},
      {"id": "uuid3", "error": null}
    ],
    "successful": 3,
    "failed": 0
  }
  ```

#### Receive Messages
- **POST** `/queues/{queue_name}/messages/get`
- **Body**:
  ```json
  {
    "count": 5  // optional, defaults to 1
  }
  ```
- **Returns**: Array of messages with receipt handles

#### Delete Message
- **DELETE** `/queues/{queue_name}/messages/{receipt_handle}`
- **Description**: Confirms message processing and removes it from the queue
- **Returns**: 204 No Content on success

## Configuration

### Queue Settings

- **`visibility_timeout_seconds`**: Duration (in seconds) that a message remains invisible after being retrieved. Default: 120 seconds (2 minutes)

### Storage

- Queue specifications are stored in `./queue_specs/` as JSON files
- Messages are kept in memory for maximum performance
- Queue configurations persist across server restarts

## Architecture

### Thread Safety

RSQueue uses Rust's `tokio::sync::RwLock` to ensure all operations are thread-safe:
- Multiple readers can access queue information simultaneously
- Write operations (enqueue, dequeue, delete) acquire exclusive locks
- Dequeue operations atomically handle expired messages and new retrievals

### Message Lifecycle

1. **Enqueued**: Message added to queue, receives unique ID and optional TTL
2. **Available**: Message visible and ready for processing
3. **In-Flight**: Message retrieved by consumer, invisible to others
4. **Deleted**: Message successfully processed and removed
5. **Re-queued**: Message returns to Available if not deleted within visibility timeout
6. **Expired**: Message with TTL automatically deleted after expiration time (regardless of state)

### Visibility Timeout

When a consumer retrieves a message:
- Message becomes invisible to other consumers
- Consumer receives a unique `receipt_handle`
- Message has `visibility_timeout_seconds` to be processed
- If not deleted within timeout, message automatically returns to queue

## Example Use Cases

### Worker Queue Pattern

```bash
# Producer adds tasks
for i in {1..100}; do
  curl -X POST http://localhost:3000/queues/jobs/messages \
    -H "Content-Type: application/json" \
    -d "{\"content\": \"Job $i\"}"
done

# Multiple workers consume tasks
while true; do
  RESPONSE=$(curl -s -X POST http://localhost:3000/queues/jobs/messages/get \
    -H "Content-Type: application/json" \
    -d '{"count": 1}')
  
  # Process message and delete if successful
  RECEIPT=$(echo $RESPONSE | jq -r '.[0].receipt_handle')
  if [ "$RECEIPT" != "null" ]; then
    # Process the job here
    echo "Processing: $(echo $RESPONSE | jq -r '.[0].content')"
    
    # Delete on success
    curl -X DELETE http://localhost:3000/queues/jobs/messages/$RECEIPT
  else
    sleep 1
  fi
done
```

### Batch Processing

```bash
# Send multiple messages at once
curl -X POST http://localhost:3000/queues/notifications/messages/batch \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      "Send email to user1@example.com",
      "Send SMS to +1234567890",
      "Push notification to device_token_xyz"
    ]
  }'

# Retrieve and process in batches
curl -X POST http://localhost:3000/queues/notifications/messages/get \
  -H "Content-Type: application/json" \
  -d '{"count": 10}'
```

## Performance Considerations

- **In-Memory Storage**: All messages are stored in memory for maximum performance
- **Persistence**: Only queue specifications are persisted to disk
- **Scalability**: Single-node design, suitable for millions of messages depending on message size and available RAM
- **Concurrency**: Optimized for high-concurrency scenarios with multiple producers/consumers

## Development

### Running Tests

```bash
cargo test
```

### Building for Production

```bash
cargo build --release
```

### Docker Deployment

```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/rsqueue .
EXPOSE 3000
CMD ["./rsqueue"]
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - feel free to use this in your projects!

## Roadmap

- [ ] Add message TTL (time-to-live)
- [ ] Implement message priorities
- [ ] Add dead letter queue support
- [ ] Create distributed version with clustering
- [ ] Add message compression
- [ ] Implement persistence options (RocksDB, PostgreSQL)
- [ ] Add metrics and monitoring endpoints
- [ ] WebSocket support for real-time message streaming
- [ ] Message deduplication
- [ ] Scheduled message delivery

## Support

For issues, questions, or suggestions, please open an issue on GitHub.