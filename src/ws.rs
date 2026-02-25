use axum::{
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::Response,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{AppState, Message, QueueEvent};

// --- Protocol types ---

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

// --- Session state ---

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
            QueueEvent::MessageMovedToDLQ {
                source_queue,
                dlq_queue,
                ..
            } => {
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

    /// Validate a queue_name target against the scoped queue, returning an error message if invalid.
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

// --- Axum handlers ---

pub async fn ws_handler(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_ws_connection(socket, state, None))
}

pub async fn ws_queue_handler(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_ws_connection(socket, state, Some(queue_name)))
}

// --- Core connection loop ---

async fn handle_ws_connection(socket: WebSocket, state: AppState, scoped_queue: Option<String>) {
    use futures::stream::StreamExt;
    use futures::SinkExt;
    use tokio_stream::wrappers::BroadcastStream;

    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Channel for outbound messages — handlers send here, a task drains to ws_sender
    let (tx, mut rx) = mpsc::unbounded_channel::<WsServerMessage>();

    // Send task: forward channel → WebSocket
    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Ok(json) = serde_json::to_string(&msg) {
                if ws_sender.send(WsMessage::Text(json.into())).await.is_err() {
                    break;
                }
            }
        }
    });

    // Send connected message
    let mut session = WsSession::new(scoped_queue.clone());

    let _ = tx.send(WsServerMessage::Connected {
        server_version: env!("CARGO_PKG_VERSION").to_string(),
        timestamp: Utc::now(),
    });

    // If scoped to a queue, auto-subscribe
    if let Some(ref qn) = scoped_queue {
        session.subscribed_queues.insert(qn.clone());
        let _ = tx.send(WsServerMessage::Subscribed {
            scope: format!("queue:{}", qn),
        });
    }

    // Subscribe to broadcast events
    let mut broadcast_rx = BroadcastStream::new(state.event_broadcaster.subscribe());

    // Heartbeat interval
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            // Inbound client message
            maybe_msg = ws_receiver.next() => {
                match maybe_msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        handle_client_message(&text, &mut session, &state, &tx).await;
                    }
                    Some(Ok(WsMessage::Close(_))) | None => break,
                    _ => {} // ignore binary, ping/pong handled by axum
                }
            }

            // Broadcast event
            maybe_event = broadcast_rx.next() => {
                match maybe_event {
                    Some(Ok(event)) => {
                        if session.should_forward_event(&event) {
                            let _ = tx.send(WsServerMessage::Event { event });
                        }
                    }
                    Some(Err(_)) => {} // lagged, skip
                    None => break,     // broadcaster dropped
                }
            }

            // Heartbeat
            _ = heartbeat.tick() => {
                let _ = tx.send(WsServerMessage::Pong { timestamp: Utc::now() });
            }
        }
    }

    // Shutdown
    drop(tx);
    let _ = send_task.await;
}

// --- Message dispatch ---

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

                // Handle DLQ messages if any
                let dlq_moved = result.dlq_messages.len();
                if !result.dlq_messages.is_empty() {
                    if let Some(ref dlq_name) = queue.spec.dead_letter_queue.clone() {
                        if let Some(dlq) = queues.get_mut(dlq_name) {
                            for dlq_msg in &result.dlq_messages {
                                let _ = dlq.enqueue(
                                    dlq_msg.get_content_string(),
                                    None,
                                    None,
                                    Some(dlq_msg.priority),
                                );
                                state.event_broadcaster.broadcast(
                                    QueueEvent::MessageMovedToDLQ {
                                        source_queue: queue_name.clone(),
                                        dlq_queue: dlq_name.clone(),
                                        message_id: dlq_msg.id,
                                        delivery_count: dlq_msg.delivery_count,
                                        timestamp: Utc::now(),
                                    },
                                );
                            }
                        }
                    }
                }

                // Broadcast dequeue event
                if !result.messages.is_empty() {
                    let queue_depth =
                        queues.get(&queue_name).map(|q| q.size()).unwrap_or(0);
                    state.event_broadcaster.broadcast(QueueEvent::MessagesDequeued {
                        queue_name: queue_name.clone(),
                        count: result.messages.len(),
                        queue_depth,
                        timestamp: Utc::now(),
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

        WsClientMessage::Ack {
            queue_name,
            receipt_handle,
        } => {
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
                        timestamp: Utc::now(),
                    });
                    let _ = tx.send(WsServerMessage::AckOk {
                        queue_name,
                        receipt_handle,
                    });
                } else {
                    let _ = tx.send(WsServerMessage::Error {
                        code: "invalid_receipt_handle".to_string(),
                        message: format!(
                            "Receipt handle '{}' not found in queue '{}'",
                            receipt_handle, queue_name
                        ),
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

        WsClientMessage::Nack {
            queue_name,
            receipt_handle,
        } => {
            if let Some(err) = session.validate_scope(&queue_name, "nack") {
                let _ = tx.send(err);
                return;
            }
            let mut queues = state.queues.write().await;
            if let Some(queue) = queues.get_mut(&queue_name) {
                if queue.nack_message(receipt_handle) {
                    let _ = tx.send(WsServerMessage::NackOk {
                        queue_name,
                        receipt_handle,
                    });
                } else {
                    let _ = tx.send(WsServerMessage::Error {
                        code: "invalid_receipt_handle".to_string(),
                        message: format!(
                            "Receipt handle '{}' not found in queue '{}'",
                            receipt_handle, queue_name
                        ),
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
            let _ = tx.send(WsServerMessage::Pong {
                timestamp: Utc::now(),
            });
        }
    }
}
