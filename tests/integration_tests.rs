use axum::{
    http::StatusCode,
    routing::{delete, get, post},
    Router,
};
use axum_test::TestServer;
use rsqueue::*;
use std::{
    env,
    time::Duration as StdDuration,
};
use uuid::Uuid;

fn create_test_queue() -> Queue {
    Queue::new("test_queue".to_string(), 120, false, 300, None, None, None)
}

fn create_test_queue_with_dedup() -> Queue {
    Queue::new("test_dedup_queue".to_string(), 120, true, 300, None, None, None)
}

#[tokio::test]
async fn test_queue_creation() {
    let queue = create_test_queue();
    assert_eq!(queue.spec.name, "test_queue");
    assert_eq!(queue.spec.visibility_timeout_seconds, 120);
    assert!(!queue.spec.enable_deduplication);
    assert_eq!(queue.size(), 0);
}

#[tokio::test]
async fn test_enqueue_dequeue() {
    let mut queue = create_test_queue();

    let id = queue.enqueue("test message".to_string(), None, None, None).unwrap();
    assert_eq!(queue.size(), 1);

    let messages = queue.dequeue(1).messages;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].content.as_deref(), Some("test message"));
    assert_eq!(messages[0].id, id);
    assert!(messages[0].receipt_handle.is_some());

    assert_eq!(queue.size(), 1); // Still 1 because message is in-flight
}

#[tokio::test]
async fn test_batch_enqueue() {
    let mut queue = create_test_queue();

    let messages = vec![
        BatchMessageRequest { content: "msg1".to_string(), ttl_seconds: None, delay_seconds: None, priority: None },
        BatchMessageRequest { content: "msg2".to_string(), ttl_seconds: None, delay_seconds: None, priority: None },
        BatchMessageRequest { content: "msg3".to_string(), ttl_seconds: None, delay_seconds: None, priority: None },
    ];
    let results = queue.enqueue_batch(messages);

    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|r| r.is_ok()));
    assert_eq!(queue.size(), 3);
}

#[tokio::test]
async fn test_deduplication() {
    let mut queue = create_test_queue_with_dedup();

    let id1 = queue.enqueue("duplicate message".to_string(), None, None, None).unwrap();
    let result2 = queue.enqueue("duplicate message".to_string(), None, None, None);

    assert!(result2.is_err());
    assert_eq!(queue.size(), 1);

    let messages = queue.dequeue(2).messages;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, id1);
}

#[tokio::test]
async fn test_message_deletion() {
    let mut queue = create_test_queue();

    queue.enqueue("test message".to_string(), None, None, None).unwrap();
    let messages = queue.dequeue(1).messages;
    let receipt_handle = messages[0].receipt_handle.unwrap();

    assert_eq!(queue.size(), 1);

    let deleted = queue.delete_message(receipt_handle);
    assert!(deleted);
    assert_eq!(queue.size(), 0);
}

#[tokio::test]
async fn test_visibility_timeout() {
    let mut queue = Queue::new("test".to_string(), 1, false, 300, None, None, None); // 1 second timeout

    queue.enqueue("test message".to_string(), None, None, None).unwrap();
    let messages = queue.dequeue(1).messages;
    assert_eq!(messages.len(), 1);
    assert_eq!(queue.size(), 1); // In-flight

    // Wait for visibility timeout
    tokio::time::sleep(StdDuration::from_secs(2)).await;

    // Message should be available again
    let messages2 = queue.dequeue(1).messages;
    assert_eq!(messages2.len(), 1);
    assert_eq!(messages2[0].content.as_deref(), Some("test message"));
}

#[tokio::test]
async fn test_queue_purge() {
    let mut queue = create_test_queue();

    queue.enqueue("msg1".to_string(), None, None, None).unwrap();
    queue.enqueue("msg2".to_string(), None, None, None).unwrap();
    let _messages = queue.dequeue(1).messages;
    assert_eq!(queue.size(), 2); // 1 pending + 1 in-flight

    queue.purge();
    assert_eq!(queue.size(), 0);
}

#[tokio::test]
async fn test_visible_count() {
    let mut queue = Queue::new("test".to_string(), 1, false, 300, None, None, None);

    queue.enqueue("msg1".to_string(), None, None, None).unwrap();
    queue.enqueue("msg2".to_string(), None, None, None).unwrap();
    assert_eq!(queue.get_visible_count(), 2);

    queue.dequeue(1).messages;
    assert_eq!(queue.get_visible_count(), 1); // 1 pending, 1 in-flight (not visible)

    // Wait for visibility timeout
    tokio::time::sleep(StdDuration::from_secs(2)).await;
    assert_eq!(queue.get_visible_count(), 2); // Both should be visible now
}

#[tokio::test]
async fn test_content_hash() {
    let hash1 = Queue::compute_content_hash("test message");
    let hash2 = Queue::compute_content_hash("test message");
    let hash3 = Queue::compute_content_hash("different message");
    
    assert_eq!(hash1, hash2);
    assert_ne!(hash1, hash3);
}

async fn create_test_app() -> TestServer {
    let temp_path = env::temp_dir().join(format!("rsqueue_test_{}", uuid::Uuid::new_v4()));
    let state = AppState::new(temp_path);
    
    let app = Router::new()
        .route("/queues", post(create_queue))
        .route("/queues", get(list_queues))
        .route("/queues/:name", delete(delete_queue))
        .route("/queues/:name/purge", post(purge_queue))
        .route("/queues/:name/messages", post(enqueue_message))
        .route("/queues/:name/messages/batch", post(enqueue_batch))
        .route("/queues/:name/messages/get", post(get_messages))
        .route("/queues/:name/messages/:receipt_handle", delete(delete_message))
        .with_state(state);
    
    TestServer::new(app).unwrap()
}

#[tokio::test]
async fn test_create_queue_endpoint() {
    let server = create_test_app().await;
    
    let request = CreateQueueRequest {
        name: "test_queue".to_string(),
        visibility_timeout_seconds: 300,
        enable_deduplication: false,
        deduplication_window_seconds: 600,
        default_message_ttl_seconds: None,
        dead_letter_queue: None,
        max_receive_count: None,
    };
    
    let response = server
        .post("/queues")
        .json(&request)
        .await;
    
    response.assert_status_ok();
    let spec: QueueSpec = response.json();
    assert_eq!(spec.name, "test_queue");
    assert_eq!(spec.visibility_timeout_seconds, 300);
}

#[tokio::test]
async fn test_list_queues_endpoint() {
    let server = create_test_app().await;
    
    // Create a queue first
    let request = CreateQueueRequest {
        name: "test_queue".to_string(),
        visibility_timeout_seconds: 120,
        enable_deduplication: false,
        deduplication_window_seconds: 300,
        default_message_ttl_seconds: None,
        dead_letter_queue: None,
        max_receive_count: None,
    };

    server.post("/queues").json(&request).await.assert_status_ok();
    
    // List queues
    let response = server.get("/queues").await;
    response.assert_status_ok();
    
    let queues: Vec<QueueInfo> = response.json();
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].name, "test_queue");
}

#[tokio::test]
async fn test_enqueue_message_endpoint() {
    let server = create_test_app().await;
    
    // Create a queue
    let create_request = CreateQueueRequest {
        name: "test_queue".to_string(),
        visibility_timeout_seconds: 120,
        enable_deduplication: false,
        deduplication_window_seconds: 300,
        default_message_ttl_seconds: None,
        dead_letter_queue: None,
        max_receive_count: None,
    };
    server.post("/queues").json(&create_request).await.assert_status_ok();

    // Enqueue a message
    let enqueue_request = EnqueueRequest {
        content: "test message".to_string(),
        ttl_seconds: None,
        delay_seconds: None,
        priority: None,
    };

    let response = server
        .post("/queues/test_queue/messages")
        .json(&enqueue_request)
        .await;

    response.assert_status_ok();
    let result: EnqueueResponse = response.json();
    assert!(result.error.is_none());
    assert_ne!(result.id, Uuid::nil());
}

#[tokio::test]
async fn test_get_messages_endpoint() {
    let server = create_test_app().await;

    // Create a queue
    let create_request = CreateQueueRequest {
        name: "test_queue".to_string(),
        visibility_timeout_seconds: 120,
        enable_deduplication: false,
        deduplication_window_seconds: 300,
        default_message_ttl_seconds: None,
        dead_letter_queue: None,
        max_receive_count: None,
    };
    server.post("/queues").json(&create_request).await.assert_status_ok();
    
    // Enqueue a message
    let enqueue_request = EnqueueRequest {
        content: "test message".to_string(),
        ttl_seconds: None,
        delay_seconds: None,
        priority: None,
    };
    server.post("/queues/test_queue/messages").json(&enqueue_request).await.assert_status_ok();

    // Get messages
    let get_request = GetMessagesRequest { count: 1 };
    let response = server
        .post("/queues/test_queue/messages/get")
        .json(&get_request)
        .await;
    
    response.assert_status_ok();
    let messages: Vec<Message> = response.json();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].content.as_deref(), Some("test message"));
    assert!(messages[0].receipt_handle.is_some());
}

#[tokio::test]
async fn test_delete_message_endpoint() {
    let server = create_test_app().await;

    // Create queue and enqueue message
    let create_request = CreateQueueRequest {
        name: "test_queue".to_string(),
        visibility_timeout_seconds: 120,
        enable_deduplication: false,
        deduplication_window_seconds: 300,
        default_message_ttl_seconds: None,
        dead_letter_queue: None,
        max_receive_count: None,
    };
    server.post("/queues").json(&create_request).await.assert_status_ok();
    
    let enqueue_request = EnqueueRequest {
        content: "test message".to_string(),
        ttl_seconds: None,
        delay_seconds: None,
        priority: None,
    };
    server.post("/queues/test_queue/messages").json(&enqueue_request).await.assert_status_ok();

    // Get the message
    let get_request = GetMessagesRequest { count: 1 };
    let response = server.post("/queues/test_queue/messages/get").json(&get_request).await;
    let messages: Vec<Message> = response.json();
    let receipt_handle = messages[0].receipt_handle.unwrap();

    // Delete the message
    let delete_response = server
        .delete(&format!("/queues/test_queue/messages/{}", receipt_handle))
        .await;
    
    delete_response.assert_status(StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_batch_enqueue_endpoint() {
    let server = create_test_app().await;

    // Create a queue
    let create_request = CreateQueueRequest {
        name: "test_queue".to_string(),
        visibility_timeout_seconds: 120,
        enable_deduplication: false,
        deduplication_window_seconds: 300,
        default_message_ttl_seconds: None,
        dead_letter_queue: None,
        max_receive_count: None,
    };
    server.post("/queues").json(&create_request).await.assert_status_ok();
    
    // Batch enqueue
    let batch_request = BatchEnqueueRequest {
        messages: vec![
            BatchMessageRequest { content: "message 1".to_string(), ttl_seconds: None, delay_seconds: None, priority: None },
            BatchMessageRequest { content: "message 2".to_string(), ttl_seconds: Some(60), delay_seconds: None, priority: None },
            BatchMessageRequest { content: "message 3".to_string(), ttl_seconds: None, delay_seconds: None, priority: None },
        ],
    };
    
    let response = server
        .post("/queues/test_queue/messages/batch")
        .json(&batch_request)
        .await;
    
    response.assert_status_ok();
    let result: BatchEnqueueResponse = response.json();
    assert_eq!(result.successful, 3);
    assert_eq!(result.failed, 0);
    assert_eq!(result.results.len(), 3);
}

#[tokio::test]
async fn test_purge_queue_endpoint() {
    let server = create_test_app().await;

    // Create queue and add messages
    let create_request = CreateQueueRequest {
        name: "test_queue".to_string(),
        visibility_timeout_seconds: 120,
        enable_deduplication: false,
        deduplication_window_seconds: 300,
        default_message_ttl_seconds: None,
        dead_letter_queue: None,
        max_receive_count: None,
    };
    server.post("/queues").json(&create_request).await.assert_status_ok();
    
    let enqueue_request = EnqueueRequest {
        content: "test message".to_string(),
        ttl_seconds: None,
        delay_seconds: None,
        priority: None,
    };
    server.post("/queues/test_queue/messages").json(&enqueue_request).await.assert_status_ok();

    // Purge the queue
    let response = server.post("/queues/test_queue/purge").await;
    response.assert_status(StatusCode::NO_CONTENT);
    
    // Verify queue is empty
    let list_response = server.get("/queues").await;
    let queues: Vec<QueueInfo> = list_response.json();
    assert_eq!(queues[0].size, 0);
}

#[tokio::test]
async fn test_delete_queue_endpoint() {
    let server = create_test_app().await;

    // Create a queue
    let create_request = CreateQueueRequest {
        name: "test_queue".to_string(),
        visibility_timeout_seconds: 120,
        enable_deduplication: false,
        deduplication_window_seconds: 300,
        default_message_ttl_seconds: None,
        dead_letter_queue: None,
        max_receive_count: None,
    };
    server.post("/queues").json(&create_request).await.assert_status_ok();
    
    // Delete the queue
    let response = server.delete("/queues/test_queue").await;
    response.assert_status(StatusCode::NO_CONTENT);
    
    // Verify queue is gone
    let list_response = server.get("/queues").await;
    let queues: Vec<QueueInfo> = list_response.json();
    assert_eq!(queues.len(), 0);
}

#[tokio::test]
async fn test_deduplication_endpoint() {
    let server = create_test_app().await;

    // Create a queue with deduplication
    let create_request = CreateQueueRequest {
        name: "dedup_queue".to_string(),
        visibility_timeout_seconds: 120,
        enable_deduplication: true,
        deduplication_window_seconds: 300,
        default_message_ttl_seconds: None,
        dead_letter_queue: None,
        max_receive_count: None,
    };
    server.post("/queues").json(&create_request).await.assert_status_ok();
    
    // Enqueue the same message twice
    let enqueue_request = EnqueueRequest {
        content: "duplicate message".to_string(),
        ttl_seconds: None,
        delay_seconds: None,
        priority: None,
    };

    let response1 = server.post("/queues/dedup_queue/messages").json(&enqueue_request).await;
    response1.assert_status_ok();
    let result1: EnqueueResponse = response1.json();
    assert!(result1.error.is_none());
    
    let response2 = server.post("/queues/dedup_queue/messages").json(&enqueue_request).await;
    response2.assert_status_ok();
    let result2: EnqueueResponse = response2.json();
    assert!(result2.error.is_some());
    assert!(result2.error.unwrap().contains("Duplicate message detected"));
}

#[tokio::test]
async fn test_create_queue_with_dlq_config() {
    let server = create_test_app().await;

    // Create the DLQ first
    let dlq_request = CreateQueueRequest {
        name: "my-dlq".to_string(),
        visibility_timeout_seconds: 120,
        enable_deduplication: false,
        deduplication_window_seconds: 300,
        default_message_ttl_seconds: None,
        dead_letter_queue: None,
        max_receive_count: None,
    };
    server.post("/queues").json(&dlq_request).await.assert_status_ok();

    // Create a queue with DLQ config
    let create_request = CreateQueueRequest {
        name: "source-queue".to_string(),
        visibility_timeout_seconds: 120,
        enable_deduplication: false,
        deduplication_window_seconds: 300,
        default_message_ttl_seconds: None,
        dead_letter_queue: Some("my-dlq".to_string()),
        max_receive_count: Some(3),
    };
    let response = server.post("/queues").json(&create_request).await;
    response.assert_status_ok();
    let spec: QueueSpec = response.json();
    assert_eq!(spec.dead_letter_queue, Some("my-dlq".to_string()));
    assert_eq!(spec.max_receive_count, Some(3));
}

#[tokio::test]
async fn test_create_queue_self_referential_dlq_rejected() {
    let server = create_test_app().await;

    let create_request = CreateQueueRequest {
        name: "self-ref".to_string(),
        visibility_timeout_seconds: 120,
        enable_deduplication: false,
        deduplication_window_seconds: 300,
        default_message_ttl_seconds: None,
        dead_letter_queue: Some("self-ref".to_string()),
        max_receive_count: Some(3),
    };
    let response = server.post("/queues").json(&create_request).await;
    response.assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_dlq_full_flow() {
    // Test: enqueue -> dequeue -> timeout -> dequeue -> timeout -> moved to DLQ
    let mut source = Queue::new(
        "source".to_string(), 1, false, 300, None,
        Some("dlq".to_string()), Some(2),
    );
    let mut dlq = Queue::new("dlq".to_string(), 120, false, 300, None, None, None);

    source.enqueue("poison message".to_string(), None, None, None).unwrap();

    // First dequeue
    let result1 = source.dequeue(1);
    assert_eq!(result1.messages.len(), 1);
    assert_eq!(result1.messages[0].delivery_count, 1);

    // Simulate visibility timeout
    let mut msg1 = result1.messages[0].clone();
    let handle1 = msg1.receipt_handle.unwrap();
    msg1.visible_after = Some(chrono::Utc::now() - chrono::Duration::seconds(10));
    source.in_flight.insert(handle1, msg1);

    // Second dequeue - re-queued then dequeued again
    let result2 = source.dequeue(1);
    assert_eq!(result2.messages.len(), 1);
    assert_eq!(result2.messages[0].delivery_count, 2);

    // Simulate visibility timeout again
    let mut msg2 = result2.messages[0].clone();
    let handle2 = msg2.receipt_handle.unwrap();
    msg2.visible_after = Some(chrono::Utc::now() - chrono::Duration::seconds(10));
    source.in_flight.insert(handle2, msg2);

    // Third dequeue - should be flagged for DLQ
    let result3 = source.dequeue(1);
    assert_eq!(result3.messages.len(), 0);
    assert_eq!(result3.dlq_messages.len(), 1);

    // Move to DLQ (simulating what handler does)
    for mut dlq_msg in result3.dlq_messages {
        dlq_msg.receipt_handle = None;
        dlq_msg.visible_after = None;
        dlq.push_message(dlq_msg);
    }

    // Verify message is in DLQ
    assert_eq!(dlq.pending_count(), 1);
    let dlq_msgs = dlq.dequeue(1).messages;
    assert_eq!(dlq_msgs[0].content.as_deref(), Some("poison message"));
    assert_eq!(dlq_msgs[0].delivery_count, 3); // incremented when dequeued from DLQ

    // Source should be empty
    assert_eq!(source.size(), 0);
}

#[tokio::test]
async fn test_in_memory_compression() {
    let mut queue = Queue::new(
        "compressed_queue".to_string(),
        120, false, 300, None, None, None,
    );
    // Manually enable compression since our test constructor doesn't take it
    queue.spec.enable_compression = true;

    // Create a large, compressible payload
    let large_payload = "A".repeat(10_000);
    
    let msg_id = queue.enqueue(large_payload.clone(), None, None, None).unwrap();
    
    // Dequeue and ensure it comes out exactly the same
    let dequeue_result = queue.dequeue(1);
    assert_eq!(dequeue_result.messages.len(), 1);
    assert_eq!(dequeue_result.messages[0].id, msg_id);
    assert_eq!(dequeue_result.messages[0].content.as_deref().unwrap(), &large_payload);
    
    // Check internal storage size if possible (optional, but we know the API works)
}