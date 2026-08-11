use axum::{
    extract::{
        ws::{Message as WsMessage},
        Path, State,
    },
    response::Response,
    routing::{get, post, delete},
    Router,
};
use futures_util::{SinkExt, StreamExt};
use rsqueue::*;
use serde_json::{json, Value};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio_tungstenite::connect_async;

// We need to replicate the ws module handlers here since they live in the binary crate.
// Instead, we build a minimal server that includes the ws module logic inline.
// However, the ws module is in src/ws.rs which is part of the bin crate.
// We'll build the WebSocket handler directly for tests.

mod ws_handler {
    use super::*;
    use axum::extract::ws::{WebSocket, WebSocketUpgrade};
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use std::collections::HashSet;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    #[derive(Debug, Deserialize)]
    #[serde(tag = "type")]
    enum WsClientMessage {
        #[serde(rename = "subscribe_all")]
        SubscribeAll,
        #[serde(rename = "subscribe_queue")]
        SubscribeQueue { queue_name: String },
        #[serde(rename = "unsubscribe_queue")]
        UnsubscribeQueue { queue_name: String },
        #[serde(rename = "consume")]
        Consume {
            queue_name: String,
            count: Option<usize>,
        },
        #[serde(rename = "ack")]
        Ack {
            queue_name: String,
            receipt_handle: Uuid,
        },
        #[serde(rename = "nack")]
        Nack {
            queue_name: String,
            receipt_handle: Uuid,
        },
        #[serde(rename = "ping")]
        Ping,
    }

    #[derive(Debug, Serialize)]
    #[serde(tag = "type")]
    enum WsServerMessage {
        #[serde(rename = "connected")]
        Connected {
            server_version: String,
            timestamp: DateTime<Utc>,
        },
        #[serde(rename = "event")]
        Event { event: QueueEvent },
        #[serde(rename = "messages")]
        Messages {
            queue_name: String,
            messages: Vec<Message>,
            dlq_messages_moved: usize,
        },
        #[serde(rename = "ack_ok")]
        AckOk {
            queue_name: String,
            receipt_handle: Uuid,
        },
        #[serde(rename = "nack_ok")]
        NackOk {
            queue_name: String,
            receipt_handle: Uuid,
        },
        #[serde(rename = "subscribed")]
        Subscribed { scope: String },
        #[serde(rename = "unsubscribed")]
        Unsubscribed { queue_name: String },
        #[serde(rename = "error")]
        Error {
            code: String,
            message: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            request_type: Option<String>,
        },
        #[serde(rename = "pong")]
        Pong { timestamp: DateTime<Utc> },
    }

    struct WsSession {
        subscribed_all: bool,
        subscribed_queues: HashSet<String>,
        scoped_queue: Option<String>,
    }

    impl WsSession {
        fn new(scoped_queue: Option<String>) -> Self {
            Self {
                subscribed_all: false,
                subscribed_queues: HashSet::new(),
                scoped_queue,
            }
        }

        fn should_forward_event(&self, event: &QueueEvent) -> bool {
            if self.subscribed_all {
                return true;
            }
            let queue_name = match event {
                QueueEvent::MessageEnqueued { queue_name, .. } => Some(queue_name.as_str()),
                QueueEvent::MessagesDequeued { queue_name, .. } => Some(queue_name.as_str()),
                QueueEvent::MessageDeleted { queue_name, .. } => Some(queue_name.as_str()),
                QueueEvent::BatchDeleted { queue_name, .. } => Some(queue_name.as_str()),
                QueueEvent::QueuePurged { queue_name, .. } => Some(queue_name.as_str()),
                QueueEvent::QueueDeleted { queue_name, .. } => Some(queue_name.as_str()),
                QueueEvent::QueueCreated { queue_name, .. } => Some(queue_name.as_str()),
                QueueEvent::MessagesEvicted { queue_name, .. } => Some(queue_name.as_str()),
                QueueEvent::MessageMovedToDLQ { source_queue, dlq_queue, .. } => {
                    if self.subscribed_queues.contains(source_queue.as_str())
                        || self.subscribed_queues.contains(dlq_queue.as_str())
                    {
                        return true;
                    }
                    None
                }
                QueueEvent::Heartbeat { .. } | QueueEvent::MetricsUpdate { .. } => return true,
            };
            if let Some(qn) = queue_name {
                self.subscribed_queues.contains(qn)
            } else {
                false
            }
        }

        fn validate_scope(&self, queue_name: &str, request_type: &str) -> Option<WsServerMessage> {
            if let Some(ref scoped) = self.scoped_queue {
                if queue_name != scoped {
                    return Some(WsServerMessage::Error {
                        code: "scope_violation".to_string(),
                        message: format!(
                            "This connection is scoped to queue '{}', cannot target '{}'",
                            scoped, queue_name
                        ),
                        request_type: Some(request_type.to_string()),
                    });
                }
            }
            None
        }
    }

    pub async fn ws_global(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
        ws.on_upgrade(move |socket| handle_connection(socket, state, None))
    }

    pub async fn ws_queue(
        State(state): State<AppState>,
        Path(queue_name): Path<String>,
        ws: WebSocketUpgrade,
    ) -> Response {
        ws.on_upgrade(move |socket| handle_connection(socket, state, Some(queue_name)))
    }

    async fn handle_connection(socket: WebSocket, state: AppState, scoped_queue: Option<String>) {
        use futures::stream::StreamExt;
        use futures::SinkExt;
        use tokio_stream::wrappers::BroadcastStream;

        let (mut ws_sender, mut ws_receiver) = socket.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<WsServerMessage>();

        let send_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Ok(json) = serde_json::to_string(&msg) {
                    if ws_sender.send(WsMessage::Text(json.into())).await.is_err() {
                        break;
                    }
                }
            }
        });

        let mut session = WsSession::new(scoped_queue.clone());

        let _ = tx.send(WsServerMessage::Connected {
            server_version: env!("CARGO_PKG_VERSION").to_string(),
            timestamp: Utc::now(),
        });

        if let Some(ref qn) = scoped_queue {
            session.subscribed_queues.insert(qn.clone());
            let _ = tx.send(WsServerMessage::Subscribed {
                scope: format!("queue:{}", qn),
            });
        }

        let mut broadcast_rx = BroadcastStream::new(state.event_broadcaster.subscribe());
        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
        heartbeat.tick().await;

        loop {
            tokio::select! {
                maybe_msg = ws_receiver.next() => {
                    match maybe_msg {
                        Some(Ok(WsMessage::Text(text))) => {
                            handle_client_message(&text, &mut session, &state, &tx).await;
                        }
                        Some(Ok(WsMessage::Close(_))) | None => break,
                        _ => {}
                    }
                }
                maybe_event = broadcast_rx.next() => {
                    match maybe_event {
                        Some(Ok(event)) => {
                            if session.should_forward_event(&event) {
                                let _ = tx.send(WsServerMessage::Event { event });
                            }
                        }
                        Some(Err(_)) => {}
                        None => break,
                    }
                }
                _ = heartbeat.tick() => {
                    let _ = tx.send(WsServerMessage::Pong { timestamp: Utc::now() });
                }
            }
        }

        drop(tx);
        let _ = send_task.await;
    }

    async fn handle_client_message(
        text: &str,
        session: &mut WsSession,
        state: &AppState,
        tx: &mpsc::UnboundedSender<WsServerMessage>,
    ) {
        let msg: WsClientMessage = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(e) => {
                let _ = tx.send(WsServerMessage::Error {
                    code: "invalid_message".to_string(),
                    message: format!("Failed to parse message: {}", e),
                    request_type: None,
                });
                return;
            }
        };

        match msg {
            WsClientMessage::SubscribeAll => {
                if session.scoped_queue.is_some() {
                    let _ = tx.send(WsServerMessage::Error {
                        code: "scope_violation".to_string(),
                        message: "Cannot subscribe_all on a queue-scoped connection".to_string(),
                        request_type: Some("subscribe_all".to_string()),
                    });
                    return;
                }
                session.subscribed_all = true;
                let _ = tx.send(WsServerMessage::Subscribed {
                    scope: "all".to_string(),
                });
            }
            WsClientMessage::SubscribeQueue { queue_name } => {
                if let Some(err) = session.validate_scope(&queue_name, "subscribe_queue") {
                    let _ = tx.send(err);
                    return;
                }
                session.subscribed_queues.insert(queue_name.clone());
                let _ = tx.send(WsServerMessage::Subscribed {
                    scope: format!("queue:{}", queue_name),
                });
            }
            WsClientMessage::UnsubscribeQueue { queue_name } => {
                if let Some(err) = session.validate_scope(&queue_name, "unsubscribe_queue") {
                    let _ = tx.send(err);
                    return;
                }
                session.subscribed_queues.remove(&queue_name);
                let _ = tx.send(WsServerMessage::Unsubscribed { queue_name });
            }
            WsClientMessage::Consume { queue_name, count } => {
                if let Some(err) = session.validate_scope(&queue_name, "consume") {
                    let _ = tx.send(err);
                    return;
                }
                let count = count.unwrap_or(1).max(1);
                let mut queues = state.queues.write().await;
                if let Some(queue) = queues.get_mut(&queue_name) {
                    let result = queue.dequeue(count);
                    let dlq_moved = result.dlq_messages.len();
                    if !result.messages.is_empty() {
                        let queue_depth = queues.get(&queue_name).map(|q| q.size()).unwrap_or(0);
                        state.event_broadcaster.broadcast(QueueEvent::MessagesDequeued {
                            queue_name: queue_name.clone(),
                            count: result.messages.len(),
                            queue_depth,
                            timestamp: chrono::Utc::now(),
                        });
                    }
                    let _ = tx.send(WsServerMessage::Messages {
                        queue_name,
                        messages: result.messages,
                        dlq_messages_moved: dlq_moved,
                    });
                } else {
                    let _ = tx.send(WsServerMessage::Error {
                        code: "queue_not_found".to_string(),
                        message: format!("Queue '{}' does not exist", queue_name),
                        request_type: Some("consume".to_string()),
                    });
                }
            }
            WsClientMessage::Ack { queue_name, receipt_handle } => {
                if let Some(err) = session.validate_scope(&queue_name, "ack") {
                    let _ = tx.send(err);
                    return;
                }
                let mut queues = state.queues.write().await;
                if let Some(queue) = queues.get_mut(&queue_name) {
                    if queue.delete_message(receipt_handle) {
                        let queue_depth = queue.size();
                        drop(queues);
                        state.event_broadcaster.broadcast(QueueEvent::MessageDeleted {
                            queue_name: queue_name.clone(),
                            receipt_handle,
                            queue_depth,
                            timestamp: chrono::Utc::now(),
                        });
                        let _ = tx.send(WsServerMessage::AckOk { queue_name, receipt_handle });
                    } else {
                        let _ = tx.send(WsServerMessage::Error {
                            code: "invalid_receipt_handle".to_string(),
                            message: format!("Receipt handle '{}' not found in queue '{}'", receipt_handle, queue_name),
                            request_type: Some("ack".to_string()),
                        });
                    }
                } else {
                    let _ = tx.send(WsServerMessage::Error {
                        code: "queue_not_found".to_string(),
                        message: format!("Queue '{}' does not exist", queue_name),
                        request_type: Some("ack".to_string()),
                    });
                }
            }
            WsClientMessage::Nack { queue_name, receipt_handle } => {
                if let Some(err) = session.validate_scope(&queue_name, "nack") {
                    let _ = tx.send(err);
                    return;
                }
                let mut queues = state.queues.write().await;
                if let Some(queue) = queues.get_mut(&queue_name) {
                    if queue.nack_message(receipt_handle) {
                        let _ = tx.send(WsServerMessage::NackOk { queue_name, receipt_handle });
                    } else {
                        let _ = tx.send(WsServerMessage::Error {
                            code: "invalid_receipt_handle".to_string(),
                            message: format!("Receipt handle '{}' not found in queue '{}'", receipt_handle, queue_name),
                            request_type: Some("nack".to_string()),
                        });
                    }
                } else {
                    let _ = tx.send(WsServerMessage::Error {
                        code: "queue_not_found".to_string(),
                        message: format!("Queue '{}' does not exist", queue_name),
                        request_type: Some("nack".to_string()),
                    });
                }
            }
            WsClientMessage::Ping => {
                let _ = tx.send(WsServerMessage::Pong { timestamp: chrono::Utc::now() });
            }
        }
    }
}

use chrono::Utc;

async fn start_test_server() -> (SocketAddr, AppState) {
    let temp_path = std::env::temp_dir().join(format!("rsqueue_ws_test_{}", uuid::Uuid::new_v4()));
    let state = AppState::new(temp_path);

    let app = Router::new()
        .route("/ws", get(ws_handler::ws_global))
        .route("/queues/:name/ws", get(ws_handler::ws_queue))
        .route("/queues", post(create_queue))
        .route("/queues/:name", delete(delete_queue))
        .route("/queues/:name/messages", post(enqueue_message))
        .route("/queues/:name/messages/get", post(get_messages))
        .with_state(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (addr, state)
}

async fn ws_connect(
    addr: SocketAddr,
    path: &str,
) -> (
    futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tokio_tungstenite::tungstenite::Message,
    >,
    futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
) {
    let url = format!("ws://{}{}", addr, path);
    let (stream, _) = connect_async(&url).await.expect("Failed to connect");
    stream.split()
}

async fn send_json(
    sink: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tokio_tungstenite::tungstenite::Message,
    >,
    value: Value,
) {
    sink.send(tokio_tungstenite::tungstenite::Message::Text(
        value.to_string().into(),
    ))
    .await
    .unwrap();
}

async fn recv_json(
    stream: &mut futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
) -> Value {
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next());
    match timeout.await {
        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
            serde_json::from_str(&text).expect("Invalid JSON from server")
        }
        Ok(other) => panic!("Unexpected WebSocket message: {:?}", other),
        Err(_) => panic!("Timed out waiting for WebSocket message"),
    }
}

#[tokio::test]
async fn test_ws_connect_receives_connected_message() {
    let (addr, _state) = start_test_server().await;
    let (mut _sink, mut stream) = ws_connect(addr, "/ws").await;

    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "connected");
    assert!(msg["server_version"].is_string());
    assert!(msg["timestamp"].is_string());
}

#[tokio::test]
async fn test_ws_subscribe_all() {
    let (addr, _state) = start_test_server().await;
    let (mut sink, mut stream) = ws_connect(addr, "/ws").await;

    // Read connected message
    let _ = recv_json(&mut stream).await;

    // Subscribe all
    send_json(&mut sink, json!({"type": "subscribe_all"})).await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "subscribed");
    assert_eq!(msg["scope"], "all");
}

#[tokio::test]
async fn test_ws_subscribe_queue() {
    let (addr, _state) = start_test_server().await;
    let (mut sink, mut stream) = ws_connect(addr, "/ws").await;

    let _ = recv_json(&mut stream).await;

    send_json(
        &mut sink,
        json!({"type": "subscribe_queue", "queue_name": "test-q"}),
    )
    .await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "subscribed");
    assert_eq!(msg["scope"], "queue:test-q");

    // Unsubscribe
    send_json(
        &mut sink,
        json!({"type": "unsubscribe_queue", "queue_name": "test-q"}),
    )
    .await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "unsubscribed");
    assert_eq!(msg["queue_name"], "test-q");
}

#[tokio::test]
async fn test_ws_ping_pong() {
    let (addr, _state) = start_test_server().await;
    let (mut sink, mut stream) = ws_connect(addr, "/ws").await;

    let _ = recv_json(&mut stream).await;

    send_json(&mut sink, json!({"type": "ping"})).await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "pong");
    assert!(msg["timestamp"].is_string());
}

#[tokio::test]
async fn test_ws_consume_ack_flow() {
    let (addr, state) = start_test_server().await;

    // Create a queue and enqueue a message via state directly
    {
        let mut queues = state.queues.write().await;
        let queue = Queue::new("work".to_string(), 120, false, 300, None, None, None);
        queues.insert("work".to_string(), queue);
        queues.get_mut("work").unwrap().enqueue("hello".to_string(), None, None, None).unwrap();
    }

    let (mut sink, mut stream) = ws_connect(addr, "/ws").await;
    let _ = recv_json(&mut stream).await; // connected

    // Consume
    send_json(&mut sink, json!({"type": "consume", "queue_name": "work"})).await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "messages");
    assert_eq!(msg["queue_name"], "work");

    let messages = msg["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["content"], "hello");

    let receipt_handle = messages[0]["receipt_handle"].as_str().unwrap();

    // Ack
    send_json(
        &mut sink,
        json!({"type": "ack", "queue_name": "work", "receipt_handle": receipt_handle}),
    )
    .await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "ack_ok");

    // Verify queue is now empty
    {
        let queues = state.queues.read().await;
        let queue = queues.get("work").unwrap();
        assert_eq!(queue.size(), 0);
    }
}

#[tokio::test]
async fn test_ws_nack_redelivery() {
    let (addr, state) = start_test_server().await;

    {
        let mut queues = state.queues.write().await;
        let queue = Queue::new("work".to_string(), 120, false, 300, None, None, None);
        queues.insert("work".to_string(), queue);
        queues.get_mut("work").unwrap().enqueue("retry-me".to_string(), None, None, None).unwrap();
    }

    let (mut sink, mut stream) = ws_connect(addr, "/ws").await;
    let _ = recv_json(&mut stream).await; // connected

    // Consume the message
    send_json(&mut sink, json!({"type": "consume", "queue_name": "work"})).await;
    let msg = recv_json(&mut stream).await;
    let receipt_handle = msg["messages"][0]["receipt_handle"].as_str().unwrap().to_string();

    // Nack it — should return to queue
    send_json(
        &mut sink,
        json!({"type": "nack", "queue_name": "work", "receipt_handle": receipt_handle}),
    )
    .await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "nack_ok");

    // Consume again — should get the same message back
    send_json(&mut sink, json!({"type": "consume", "queue_name": "work"})).await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "messages");
    let messages = msg["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["content"], "retry-me");
}

#[tokio::test]
async fn test_ws_consume_queue_not_found() {
    let (addr, _state) = start_test_server().await;
    let (mut sink, mut stream) = ws_connect(addr, "/ws").await;
    let _ = recv_json(&mut stream).await;

    send_json(
        &mut sink,
        json!({"type": "consume", "queue_name": "nonexistent"}),
    )
    .await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "error");
    assert_eq!(msg["code"], "queue_not_found");
}

#[tokio::test]
async fn test_ws_invalid_message() {
    let (addr, _state) = start_test_server().await;
    let (mut sink, mut stream) = ws_connect(addr, "/ws").await;
    let _ = recv_json(&mut stream).await;

    // Send invalid JSON
    sink.send(tokio_tungstenite::tungstenite::Message::Text(
        "not json".into(),
    ))
    .await
    .unwrap();
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "error");
    assert_eq!(msg["code"], "invalid_message");
}

#[tokio::test]
async fn test_ws_queue_scoped_endpoint() {
    let (addr, state) = start_test_server().await;

    {
        let mut queues = state.queues.write().await;
        let queue = Queue::new("scoped-q".to_string(), 120, false, 300, None, None, None);
        queues.insert("scoped-q".to_string(), queue);
        queues
            .get_mut("scoped-q")
            .unwrap()
            .enqueue("scoped msg".to_string(), None, None, None)
            .unwrap();
    }

    let (mut sink, mut stream) = ws_connect(addr, "/queues/scoped-q/ws").await;

    // Should get connected + auto subscribed
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "connected");

    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "subscribed");
    assert_eq!(msg["scope"], "queue:scoped-q");

    // Consume should work for the scoped queue
    send_json(
        &mut sink,
        json!({"type": "consume", "queue_name": "scoped-q"}),
    )
    .await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "messages");
    assert_eq!(msg["messages"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_ws_queue_scoped_blocks_other_queues() {
    let (addr, _state) = start_test_server().await;
    let (mut sink, mut stream) = ws_connect(addr, "/queues/scoped-q/ws").await;

    let _ = recv_json(&mut stream).await; // connected
    let _ = recv_json(&mut stream).await; // subscribed

    // Try to consume from a different queue — should get scope_violation
    send_json(
        &mut sink,
        json!({"type": "consume", "queue_name": "other-queue"}),
    )
    .await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "error");
    assert_eq!(msg["code"], "scope_violation");

    // subscribe_all should also fail on scoped connection
    send_json(&mut sink, json!({"type": "subscribe_all"})).await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "error");
    assert_eq!(msg["code"], "scope_violation");
}

#[tokio::test]
async fn test_ws_events_forwarded_to_subscriber() {
    let (addr, state) = start_test_server().await;

    let (mut sink, mut stream) = ws_connect(addr, "/ws").await;
    let _ = recv_json(&mut stream).await; // connected

    // Subscribe to all
    send_json(&mut sink, json!({"type": "subscribe_all"})).await;
    let _ = recv_json(&mut stream).await; // subscribed

    // Broadcast an event from the server
    state.event_broadcaster.broadcast(QueueEvent::QueueCreated {
        queue_name: "new-queue".to_string(),
        timestamp: Utc::now(),
    });

    // Should receive the event
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "event");
    assert_eq!(msg["event"]["type"], "queue_created");
    assert_eq!(msg["event"]["queue_name"], "new-queue");
}

#[tokio::test]
async fn test_ws_consume_empty_queue() {
    let (addr, state) = start_test_server().await;

    {
        let mut queues = state.queues.write().await;
        let queue = Queue::new("empty".to_string(), 120, false, 300, None, None, None);
        queues.insert("empty".to_string(), queue);
    }

    let (mut sink, mut stream) = ws_connect(addr, "/ws").await;
    let _ = recv_json(&mut stream).await;

    send_json(
        &mut sink,
        json!({"type": "consume", "queue_name": "empty"}),
    )
    .await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "messages");
    assert_eq!(msg["messages"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_ws_ack_invalid_receipt_handle() {
    let (addr, state) = start_test_server().await;

    {
        let mut queues = state.queues.write().await;
        let queue = Queue::new("work".to_string(), 120, false, 300, None, None, None);
        queues.insert("work".to_string(), queue);
    }

    let (mut sink, mut stream) = ws_connect(addr, "/ws").await;
    let _ = recv_json(&mut stream).await;

    let fake_handle = uuid::Uuid::new_v4().to_string();
    send_json(
        &mut sink,
        json!({"type": "ack", "queue_name": "work", "receipt_handle": fake_handle}),
    )
    .await;
    let msg = recv_json(&mut stream).await;
    assert_eq!(msg["type"], "error");
    assert_eq!(msg["code"], "invalid_receipt_handle");
}
