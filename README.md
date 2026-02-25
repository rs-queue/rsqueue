# RSQueue

A high-performance, thread-safe message queue service written in Rust with HTTP API access. RSQueue provides reliable FIFO message processing with visibility timeouts, atomic operations, message deduplication, and persistent queue specifications.

## Features

- 🚀 **High Performance**: In-memory message storage with Rust's zero-cost abstractions
- 🔒 **Thread-Safe**: Fully atomic operations using RwLock for concurrent access
- ⏱️ **Visibility Timeout**: Messages become invisible to other consumers for a configurable duration
- 🔁 **Message Deduplication**: Optional content-based deduplication using SHA-256 hashing
- ⏳ **Message TTL (Time To Live)**: Optional message expiration - messages automatically deleted after specified time
- ⏰ **Delayed/Scheduled Messages**: Schedule messages for future delivery with delay_seconds parameter
- 🔢 **Message Priorities**: Priority levels 0-9 (higher = higher priority), dequeued highest-first with FIFO within same level
- 💀 **Dead Letter Queue**: Automatically move poison messages to a DLQ after configurable max receive count
- 📝 **Persistent Queue Specs**: Queue configurations survive server restarts
- 🔄 **Automatic Re-queueing**: Unprocessed messages automatically return to the queue
- 📦 **Batch Operations**: Enqueue and delete multiple messages in a single request
- 🆔 **UUID Tracking**: Unique IDs for messages and receipt handles for secure deletion
- 🗑️ **Queue Management**: Create, delete, list, and purge queues via API
- 📊 **Queue Statistics**: Track message age, throughput, and performance metrics
- 🖥️ **CLI Tool**: Command-line interface for all queue operations

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

The server will start on `http://0.0.0.0:4000`

### CLI Tool

```bash
# Build and install the CLI tool
cargo install --path . --bin rsqueue-cli

# Or run directly
cargo run --bin rsqueue-cli -- --help
```

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
curl -X POST http://localhost:4000/queues/task-queue/messages \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Process order #12345"
  }'

# Message with TTL (expires in 5 minutes)
curl -X POST http://localhost:4000/queues/task-queue/messages \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Temporary task",
    "ttl_seconds": 300
  }'

# Scheduled/delayed message (delivered in 60 seconds)
curl -X POST http://localhost:4000/queues/task-queue/messages \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Future task",
    "delay_seconds": 60
  }'

# High-priority message (priority 0-9, higher = dequeued first)
curl -X POST http://localhost:4000/queues/task-queue/messages \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Urgent task",
    "priority": 9
  }'

# Or use the CLI tool
rsqueue-cli send task-queue "Process order #12345"
rsqueue-cli send task-queue "Delayed task" --delay 60 --ttl 300
rsqueue-cli send task-queue "Urgent task" --priority 9
```

### 3. Receive Messages

```bash
curl -X POST http://localhost:4000/queues/task-queue/messages/get \
  -H "Content-Type: application/json" \
  -d '{
    "count": 1
  }'

# Or use the CLI tool
rsqueue-cli receive task-queue --count 5
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
curl -X DELETE http://localhost:4000/queues/task-queue/messages/650e8400-e29b-41d4-a716-446655440001

# Or use the CLI tool
rsqueue-cli delete-message task-queue 650e8400-e29b-41d4-a716-446655440001
```

## API Reference

### Queue Management

#### Create Queue
- **POST** `/queues`
- **Body**:
  ```json
  {
    "name": "string",
    "visibility_timeout_seconds": 120,   // optional, defaults to 120
    "dead_letter_queue": "dlq-name",     // optional, target DLQ queue name
    "max_receive_count": 5               // optional, move to DLQ after N receives
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
    "ttl_seconds": 300,       // optional, message expires after this many seconds
    "delay_seconds": 60,      // optional, message delivered after this many seconds
    "priority": 5             // optional, 0-9 (higher = higher priority, default 0)
  }
  ```
- **Returns**: `{"id": "uuid"}`

#### Send Batch Messages
- **POST** `/queues/{queue_name}/messages/batch`
- **Body**:
  ```json
  {
    "messages": [
      {"content": "message1", "ttl_seconds": 60, "priority": 9},
      {"content": "message2"},
      {"content": "message3", "priority": 5}
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

#### Batch Delete Messages
- **POST** `/queues/{queue_name}/messages/batch-delete`
- **Body**:
  ```json
  {
    "receipt_handles": ["uuid1", "uuid2", "uuid3"]
  }
  ```
- **Returns**: Batch delete results with success/failure for each message

## Configuration

### Queue Settings

- **`visibility_timeout_seconds`**: Duration (in seconds) that a message remains invisible after being retrieved. Default: 120 seconds (2 minutes)
- **`enable_deduplication`**: Enable content-based message deduplication. Default: false
- **`deduplication_window_seconds`**: How long to remember message hashes for deduplication. Default: 300 seconds (5 minutes)
- **`dead_letter_queue`**: Name of the queue to move poison messages to after `max_receive_count` is exceeded. Default: none (disabled)
- **`max_receive_count`**: Number of times a message can be received before being moved to the dead letter queue. Must be >= 1. Default: none (disabled)

### Message Options

- **`ttl_seconds`**: Message expiration time in seconds (optional)
- **`delay_seconds`**: Delay before message becomes available for delivery (optional)

### Storage

- Queue specifications are stored in `./queue_specs/` as JSON files
- Messages are kept in memory for maximum performance
- Queue configurations persist across server restarts

## CLI Tool Usage

The rsqueue-cli tool provides a convenient command-line interface:

```bash
# Create a queue
rsqueue-cli create my-queue --visibility-timeout 120 --dedup

# List all queues
rsqueue-cli list

# Send a message
rsqueue-cli send my-queue "Hello World" --ttl 300 --delay 60

# Receive messages
rsqueue-cli receive my-queue --count 10

# Delete a message
rsqueue-cli delete-message my-queue <receipt-handle>

# Get queue details and statistics
rsqueue-cli details my-queue
rsqueue-cli metrics my-queue

# Peek at messages without removing them
rsqueue-cli peek my-queue --count 5

# Delete a queue
rsqueue-cli delete my-queue

# Purge all messages
rsqueue-cli purge my-queue
```

### Environment Variables

The CLI tool supports configuration via environment variables:

```bash
export RSQUEUE_URL=http://localhost:4000
export RSQUEUE_USER=admin
export RSQUEUE_PASSWORD=secret

rsqueue-cli list
```

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
  rsqueue-cli send jobs "Job $i"
done

# Multiple workers consume tasks
while true; do
  RESPONSE=$(rsqueue-cli receive jobs --count 1)

  # Process message and delete if successful
  RECEIPT=$(echo $RESPONSE | jq -r '.[0].receipt_handle')
  if [ "$RECEIPT" != "null" ]; then
    # Process the job here
    echo "Processing: $(echo $RESPONSE | jq -r '.[0].content')"

    # Delete on success
    rsqueue-cli delete-message jobs $RECEIPT
  else
    sleep 1
  fi
done
```

### Batch Processing

```bash
# Send multiple messages at once
curl -X POST http://localhost:4000/queues/notifications/messages/batch \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"content": "Send email to user1@example.com"},
      {"content": "Send SMS to +1234567890", "ttl_seconds": 300},
      {"content": "Push notification to device_token_xyz", "delay_seconds": 60}
    ]
  }'

# Retrieve and process in batches
rsqueue-cli receive notifications --count 10
```

## Performance Considerations

- **In-Memory Storage**: All messages are stored in memory for maximum performance
- **Persistence**: Only queue specifications are persisted to disk
- **Scalability**: Single-node design, suitable for millions of messages depending on message size and available RAM
- **Concurrency**: Optimized for high-concurrency scenarios with multiple producers/consumers

## WebSocket API

RSQueue supports bidirectional real-time communication via WebSocket. Clients can subscribe to events, consume messages, and ack/nack — all over a single persistent connection.

### Endpoints

| Route | Purpose |
|-------|---------|
| `GET /ws` | Global WebSocket — starts with no subscriptions, client opts in |
| `GET /queues/{name}/ws` | Queue-scoped — auto-subscribed to that queue, commands restricted to it |

### Client → Server Messages (JSON)

```jsonc
// Subscribe to all events (global endpoint only)
{"type": "subscribe_all"}

// Subscribe to a specific queue's events
{"type": "subscribe_queue", "queue_name": "my-queue"}

// Unsubscribe from a queue
{"type": "unsubscribe_queue", "queue_name": "my-queue"}

// Consume messages (dequeue)
{"type": "consume", "queue_name": "my-queue", "count": 5}

// Acknowledge a processed message (deletes it)
{"type": "ack", "queue_name": "my-queue", "receipt_handle": "uuid-here"}

// Nack a message (return to queue immediately for re-delivery)
{"type": "nack", "queue_name": "my-queue", "receipt_handle": "uuid-here"}

// Application-level keepalive
{"type": "ping"}
```

### Server → Client Messages (JSON)

- `connected` — sent on connection with `server_version` and `timestamp`
- `event` — forwarded queue event (same events as SSE)
- `messages` — response to `consume` with `messages[]` and `dlq_messages_moved`
- `ack_ok` / `nack_ok` — confirmation responses
- `subscribed` / `unsubscribed` — subscription confirmations
- `error` — error with `code`, `message`, and optional `request_type`
- `pong` — response to `ping` and automatic heartbeat every 30s

### CLI WebSocket Commands

```bash
# Listen to all events in real-time
rsqueue-cli listen

# Listen to events for a specific queue
rsqueue-cli listen --queue my-queue

# Consume messages interactively (ack/nack/skip each message)
rsqueue-cli ws-consume my-queue

# Consume with auto-ack (print and acknowledge immediately)
rsqueue-cli ws-consume my-queue --auto-ack

# Consume multiple messages at once
rsqueue-cli ws-consume my-queue --count 5 --auto-ack
```

### Example Session (websocat)

```bash
$ websocat ws://localhost:4000/ws
< {"type":"connected","server_version":"0.1.0","timestamp":"..."}

> {"type":"subscribe_all"}
< {"type":"subscribed","scope":"all"}

# (create a queue via REST in another terminal)
< {"type":"event","event":{"type":"queue_created","queue_name":"test","timestamp":"..."}}

> {"type":"consume","queue_name":"test","count":1}
< {"type":"messages","queue_name":"test","messages":[...],"dlq_messages_moved":0}

> {"type":"ack","queue_name":"test","receipt_handle":"..."}
< {"type":"ack_ok","queue_name":"test","receipt_handle":"..."}
```

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
EXPOSE 4000
CMD ["./rsqueue"]
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - feel free to use this in your projects!

## Roadmap

- [x] Add message TTL (time-to-live)
- [x] Message deduplication
- [x] Scheduled/delayed message delivery
- [x] Add metrics and monitoring endpoints
- [x] CLI tool for queue operations
- [x] Batch delete operations
- [x] Queue statistics and performance tracking
- [x] Implement message priorities
- [x] Add dead letter queue support
- [ ] Create distributed version with clustering
- [ ] Add message compression
- [ ] Implement persistence options (RocksDB, PostgreSQL)
- [x] WebSocket support for real-time message streaming

## Support

For issues, questions, or suggestions, please open an issue on GitHub.