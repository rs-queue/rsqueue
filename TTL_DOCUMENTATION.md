# Time To Live (TTL) Documentation

## Overview

RSQueue supports optional Time To Live (TTL) for messages, allowing messages to automatically expire and be deleted after a specified duration. This feature is useful for:

- Temporary messages that lose relevance after a certain time
- Preventing queue buildup from unprocessed messages
- Implementing time-sensitive tasks
- Managing storage by automatically cleaning up old messages

## How TTL Works

1. **Setting TTL**: When enqueueing a message, you can specify `ttl_seconds` to define how long the message should live
2. **Expiration**: Messages with TTL are automatically deleted when their expiration time is reached
3. **Cleanup**: Expired messages are removed during dequeue operations, ensuring efficient queue management
4. **Persistence**: TTL is applied regardless of message state (pending or in-flight)

## API Usage

### Single Message with TTL

```bash
curl -X POST http://localhost:3000/queues/my-queue/messages \
  -H "Content-Type: application/json" \
  -d '{
    "content": "This message expires in 1 hour",
    "ttl_seconds": 3600
  }'
```

### Batch Messages with Individual TTLs

```bash
curl -X POST http://localhost:3000/queues/my-queue/messages/batch \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {
        "content": "Urgent: expires in 5 minutes",
        "ttl_seconds": 300
      },
      {
        "content": "Standard message, no expiration",
        "ttl_seconds": null
      },
      {
        "content": "Daily report, expires in 24 hours",
        "ttl_seconds": 86400
      }
    ]
  }'
```

## Response Format

When retrieving messages, the `expires_at` field indicates when a message will expire:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "content": "Temporary message",
  "created_at": "2024-01-15T10:30:00Z",
  "receipt_handle": "650e8400-e29b-41d4-a716-446655440001",
  "visible_after": "2024-01-15T10:32:00Z",
  "expires_at": "2024-01-15T11:30:00Z"  // Message expires at this time
}
```

## TTL vs Visibility Timeout

It's important to understand the difference:

- **Visibility Timeout**: Temporary invisibility while a message is being processed. Message returns to queue if not deleted.
- **TTL**: Permanent deletion after the specified time, regardless of processing status.

### Example Scenario

```json
{
  "content": "Process payment",
  "ttl_seconds": 3600  // Expires in 1 hour
}
```

With a visibility timeout of 120 seconds:
- Message is retrieved at 10:00 AM
- Message is invisible until 10:02 AM (visibility timeout)
- If not deleted by 10:02 AM, message returns to queue
- Message is permanently deleted at 11:00 AM (TTL), even if never processed

## Best Practices

### 1. Choose Appropriate TTL Values

- **Short TTL (< 5 minutes)**: For real-time notifications or alerts
- **Medium TTL (5 minutes - 1 hour)**: For time-sensitive tasks
- **Long TTL (> 1 hour)**: For batch processing or daily tasks
- **No TTL**: For critical messages that must be processed

### 2. Consider Retry Logic

If using TTL, ensure your consumers can handle message expiration:

```python
# Python example
import requests
import time

def process_with_retry(queue_name, max_retries=3):
    for attempt in range(max_retries):
        response = requests.post(f"http://localhost:3000/queues/{queue_name}/messages/get",
                                json={"count": 1})

        if response.status_code == 200:
            messages = response.json()
            if messages:
                # Process message
                process_message(messages[0])
                return True

        time.sleep(1)  # Wait before retry

    return False  # No messages available or all expired
```

### 3. Monitor Expired Messages

Track metrics for expired messages to optimize TTL values:

```bash
# Check queue metrics
curl http://localhost:3000/metrics
```

### 4. Combine with Deduplication

When using both TTL and deduplication:

```json
{
  "name": "event-queue",
  "visibility_timeout_seconds": 120,
  "enable_deduplication": true,
  "deduplication_window_seconds": 300  // 5 minutes
}
```

Messages with the same content won't be duplicated within the deduplication window, and will expire based on their individual TTL.

## Implementation Details

### Storage

Messages with TTL include an `expires_at` field:

```rust
pub struct Message {
    pub id: Uuid,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,  // TTL expiration time
    // ... other fields
}
```

### Cleanup Process

Expired messages are cleaned up:
1. During dequeue operations
2. Before returning messages to consumers
3. When listing queue contents

This ensures expired messages don't consume resources or interfere with queue operations.

## Examples

### Temporary Cache Invalidation

```bash
# Cache invalidation message that expires after 5 minutes
curl -X POST http://localhost:3000/queues/cache-invalidation/messages \
  -d '{
    "content": "{\"cache_key\": \"user:123:profile\"}",
    "ttl_seconds": 300
  }'
```

### Daily Report Processing

```bash
# Report that should be processed within 24 hours
curl -X POST http://localhost:3000/queues/reports/messages \
  -d '{
    "content": "{\"report_type\": \"daily_sales\", \"date\": \"2024-01-15\"}",
    "ttl_seconds": 86400
  }'
```

### Alert Notifications

```bash
# High-priority alert that expires quickly if not processed
curl -X POST http://localhost:3000/queues/alerts/messages \
  -d '{
    "content": "{\"severity\": \"high\", \"message\": \"CPU usage > 90%\"}",
    "ttl_seconds": 60
  }'
```

## Troubleshooting

### Messages Expiring Too Quickly

If messages are expiring before being processed:
1. Increase the TTL value
2. Scale up consumers to process messages faster
3. Consider if TTL is appropriate for these messages

### Messages Not Expiring

If messages with TTL aren't being removed:
1. Verify the TTL is set correctly in the request
2. Check that dequeue operations are occurring (cleanup happens during dequeue)
3. Verify system time is synchronized correctly

### Performance Considerations

- TTL cleanup has minimal performance impact
- Cleanup occurs during normal queue operations
- No separate cleanup thread or timer needed
- Expired messages are removed lazily for efficiency