use axum::{
    response::{Json, Response, sse::{Event, KeepAlive, Sse}},
    routing::{delete, get, post, put},
    Router,
    middleware,
};
use axum::extract::{Path, State, Request};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use base64::Engine;
use chrono::{DateTime, Utc};
use futures::stream::Stream;
use prometheus::{TextEncoder, Encoder};
use rsqueue::*;
use serde::Serialize;
use std::{convert::Infallible, path::PathBuf, time::{Duration, SystemTime}};
use tokio_stream::{wrappers::BroadcastStream, StreamExt as _};
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;
use tracing::info;
use utoipa::{OpenApi, ToSchema};

// Health check and monitoring endpoints
#[derive(Serialize, ToSchema)]
struct HealthStatus {
    status: String,
    timestamp: DateTime<Utc>,
    uptime_seconds: u64,
    version: String,
    active_queues: usize,
    total_messages: usize,
}

#[derive(Serialize, ToSchema)]
struct MetricsSummary {
    timestamp: DateTime<Utc>,
    messages_enqueued_total: u64,
    messages_dequeued_total: u64,
    messages_deleted_total: u64,
    duplicate_messages_rejected_total: u64,
    messages_expired_total: u64,
    queues_created_total: u64,
    queues_deleted_total: u64,
    queues_purged_total: u64,
    active_queues: i64,
    total_messages_pending: i64,
    total_messages_in_flight: i64,
    http_requests_total: u64,
}

#[derive(Serialize, ToSchema)]
struct QueueMetrics {
    name: String,
    messages_pending: usize,
    messages_in_flight: usize,
    total_size: usize,
    total_bytes: usize,
    visible_messages: usize,
    dedup_cache_size: usize,
    created_at: DateTime<Utc>,
    visibility_timeout_seconds: u64,
    enable_deduplication: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    oldest_message_age_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    average_message_age_seconds: Option<f64>,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        health_check,
        get_metrics_summary,
        get_queue_metrics,
        create_queue,
        update_queue_settings,
        list_queues,
        delete_queue,
        purge_queue,
        enqueue_message,
        enqueue_batch,
        get_messages,
        delete_message,
        batch_delete_messages,
        peek_messages,
        get_queue_details,
        list_all_messages,
        get_queue_history,
        get_global_history,
        cleanup_expired_messages
    ),
    components(schemas(
        HealthStatus,
        MetricsSummary,
        QueueMetrics,
        Message,
        QueueSpec,
        CreateQueueRequest,
        UpdateQueueRequest,
        EnqueueRequest,
        BatchMessageRequest,
        BatchEnqueueRequest,
        EnqueueResponse,
        BatchEnqueueResponse,
        GetMessagesRequest,
        QueueInfo,
        PeekMessagesRequest,
        MessagePreview,
        MessageStatus,
        QueueDetailInfo,
        BatchDeleteRequest,
        BatchDeleteResponse,
        DeleteResult,
        TimeSeriesPoint,
        QueueTimeSeries,
        GlobalTimeSeries,
        CleanupResponse
    )),
    tags(
        (name = "health", description = "Health check and monitoring endpoints"),
        (name = "queues", description = "Queue management operations"),
        (name = "messages", description = "Message operations"),
        (name = "stats", description = "Statistics and time-series data for graphs")
    ),
    info(
        title = "RSQueue API",
        description = "A high-performance message queue service built with Rust and Axum",
        version = "0.1.0",
        contact(
            name = "RSQueue Team"
        )
    )
)]
struct ApiDoc;

static START_TIME: std::sync::OnceLock<SystemTime> = std::sync::OnceLock::new();

#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses(
        (status = 200, description = "Service health status", body = HealthStatus)
    )
)]
async fn health_check(State(state): State<AppState>) -> Json<HealthStatus> {
    state.update_global_metrics().await;
    
    let start_time = START_TIME.get_or_init(|| SystemTime::now());
    let uptime = start_time.elapsed().unwrap_or_default();
    
    let queues = state.queues.read().await;
    let total_messages: usize = queues.values()
        .map(|q| q.size())
        .sum();
    
    Json(HealthStatus {
        status: "healthy".to_string(),
        timestamp: Utc::now(),
        uptime_seconds: uptime.as_secs(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        active_queues: queues.len(),
        total_messages,
    })
}

async fn get_metrics() -> Response {
    let encoder = TextEncoder::new();
    let metric_families = get_metrics_registry().gather();
    let mut buffer = Vec::new();
    
    if encoder.encode(&metric_families, &mut buffer).is_ok() {
        let body_string = String::from_utf8_lossy(&buffer).to_string();
        Response::builder()
            .header("Content-Type", "text/plain; version=0.0.4")
            .body(body_string.into())
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body("Failed to encode metrics".into())
            .unwrap()
    }
}

#[utoipa::path(
    get,
    path = "/metrics/summary",
    tag = "health",
    responses(
        (status = 200, description = "Metrics summary", body = MetricsSummary)
    )
)]
async fn get_metrics_summary() -> Json<MetricsSummary> {
    Json(MetricsSummary {
        timestamp: Utc::now(),
        messages_enqueued_total: get_messages_enqueued_total(),
        messages_dequeued_total: get_messages_dequeued_total(),
        messages_deleted_total: get_messages_deleted_total(),
        duplicate_messages_rejected_total: get_duplicate_messages_rejected_total(),
        messages_expired_total: get_messages_expired_total(),
        queues_created_total: get_queues_created_total(),
        queues_deleted_total: get_queues_deleted_total(),
        queues_purged_total: get_queues_purged_total(),
        active_queues: get_active_queues(),
        total_messages_pending: get_total_messages_pending(),
        total_messages_in_flight: get_total_messages_in_flight(),
        http_requests_total: get_http_requests_total(),
    })
}

#[utoipa::path(
    get,
    path = "/queues/{name}/metrics",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    responses(
        (status = 200, description = "Queue metrics", body = QueueMetrics),
        (status = 404, description = "Queue not found")
    )
)]
async fn get_queue_metrics(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
) -> Result<Json<QueueMetrics>, StatusCode> {
    let queues = state.queues.read().await;
    if let Some(queue) = queues.get(&queue_name) {
        let detail_info = queue.get_detailed_info(&queue_name);
        Ok(Json(QueueMetrics {
            name: queue_name,
            messages_pending: queue.messages.len(),
            messages_in_flight: queue.in_flight.len(),
            total_size: queue.size(),
            total_bytes: queue.total_bytes(),
            visible_messages: queue.get_visible_count(),
            dedup_cache_size: queue.get_dedup_cache_size(),
            created_at: queue.spec.created_at,
            visibility_timeout_seconds: queue.spec.visibility_timeout_seconds,
            enable_deduplication: queue.spec.enable_deduplication,
            oldest_message_age_seconds: detail_info.oldest_message_age_seconds,
            average_message_age_seconds: detail_info.average_message_age_seconds,
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// Basic auth validator that compares against environment variables
#[derive(Clone)]
struct BasicAuthValidator {
    username: String,
    password: String,
}

impl BasicAuthValidator {
    fn from_env() -> Option<Self> {
        let username = std::env::var("AUTH_USER").ok()?;
        let password = std::env::var("AUTH_PASSWORD").ok()?;

        if username.is_empty() || password.is_empty() {
            return None;
        }

        Some(Self { username, password })
    }

    fn validate(&self, auth_header: Option<&str>) -> bool {
        let Some(auth_value) = auth_header else {
            return false;
        };

        // Parse Basic auth header (format: "Basic base64(username:password)")
        if let Some(encoded) = auth_value.strip_prefix("Basic ") {
            if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) {
                if let Ok(auth_str) = String::from_utf8(decoded) {
                    if let Some((user, pass)) = auth_str.split_once(':') {
                        return user == self.username && pass == self.password;
                    }
                }
            }
        }
        false
    }
}

// Middleware function for basic authentication
async fn basic_auth_middleware(
    State(validator): State<BasicAuthValidator>,
    request: Request,
    next: Next,
) -> Response {
    // Allow OPTIONS requests without authentication (for CORS preflight)
    if request.method() == Method::OPTIONS {
        return next.run(request).await;
    }

    let auth_header = request
        .headers()
        .get("Authorization")
        .and_then(|value| value.to_str().ok());

    if validator.validate(auth_header) {
        next.run(request).await
    } else {
        // Return 401 Unauthorized with WWW-Authenticate header
        Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("WWW-Authenticate", "Basic realm=\"RSQueue\"")
            .body("Authentication required".into())
            .unwrap()
    }
}

// SSE endpoint for real-time event streaming
async fn sse_events(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let receiver = state.event_broadcaster.subscribe();

    // Convert broadcast receiver to a stream
    let stream = BroadcastStream::new(receiver).filter_map(|result| {
        match result {
            Ok(event) => {
                let json = serde_json::to_string(&event).ok()?;
                Some(Ok(Event::default().data(json)))
            }
            Err(_) => None, // Skip lagged messages
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

// SSE endpoint for queue-specific events
async fn sse_queue_events(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let receiver = state.event_broadcaster.subscribe();
    let queue_name = queue_name.clone();

    // Filter events for specific queue
    let stream = BroadcastStream::new(receiver).filter_map(move |result| {
        match result {
            Ok(event) => {
                let is_relevant = match &event {
                    QueueEvent::MessageEnqueued { queue_name: qn, .. } => *qn == queue_name,
                    QueueEvent::MessagesDequeued { queue_name: qn, .. } => *qn == queue_name,
                    QueueEvent::MessageDeleted { queue_name: qn, .. } => *qn == queue_name,
                    QueueEvent::BatchDeleted { queue_name: qn, .. } => *qn == queue_name,
                    QueueEvent::QueuePurged { queue_name: qn, .. } => *qn == queue_name,
                    QueueEvent::QueueDeleted { queue_name: qn, .. } => *qn == queue_name,
                    QueueEvent::Heartbeat { .. } => true, // Always include heartbeats
                    QueueEvent::MetricsUpdate { .. } => true, // Always include metrics
                    QueueEvent::QueueCreated { .. } => false,
                };

                if is_relevant {
                    let json = serde_json::to_string(&event).ok()?;
                    Some(Ok(Event::default().data(json)))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

// Handlers with event broadcasting

async fn create_queue_with_events(
    State(state): State<AppState>,
    Json(req): Json<CreateQueueRequest>,
) -> Result<Json<QueueSpec>, StatusCode> {
    let result = create_queue(State(state.clone()), Json(req)).await;

    if let Ok(Json(ref spec)) = result {
        state.event_broadcaster.broadcast(QueueEvent::QueueCreated {
            queue_name: spec.name.clone(),
            timestamp: Utc::now(),
        });
    }

    result
}

async fn delete_queue_with_events(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
) -> StatusCode {
    let result = delete_queue(State(state.clone()), Path(queue_name.clone())).await;

    if result == StatusCode::NO_CONTENT {
        state.event_broadcaster.broadcast(QueueEvent::QueueDeleted {
            queue_name,
            timestamp: Utc::now(),
        });
    }

    result
}

async fn purge_queue_with_events(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
) -> StatusCode {
    let result = purge_queue(State(state.clone()), Path(queue_name.clone())).await;

    if result == StatusCode::NO_CONTENT {
        state.event_broadcaster.broadcast(QueueEvent::QueuePurged {
            queue_name,
            timestamp: Utc::now(),
        });
    }

    result
}

async fn enqueue_message_with_events(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<EnqueueRequest>,
) -> Result<Json<EnqueueResponse>, StatusCode> {
    let result = enqueue_message(State(state.clone()), Path(queue_name.clone()), Json(req)).await;

    if let Ok(Json(ref response)) = result {
        if response.error.is_none() {
            let queues = state.queues.read().await;
            let queue_depth = queues.get(&queue_name).map(|q| q.size()).unwrap_or(0);
            drop(queues);

            state.event_broadcaster.broadcast(QueueEvent::MessageEnqueued {
                queue_name,
                message_id: response.id,
                queue_depth,
                timestamp: Utc::now(),
            });
        }
    }

    result
}

async fn enqueue_batch_with_events(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<BatchEnqueueRequest>,
) -> Result<Json<BatchEnqueueResponse>, StatusCode> {
    let result = enqueue_batch(State(state.clone()), Path(queue_name.clone()), Json(req)).await;

    if let Ok(Json(ref response)) = result {
        let queues = state.queues.read().await;
        let queue_depth = queues.get(&queue_name).map(|q| q.size()).unwrap_or(0);
        drop(queues);

        // Broadcast individual events for each successful enqueue
        for enqueue_result in &response.results {
            if enqueue_result.error.is_none() {
                state.event_broadcaster.broadcast(QueueEvent::MessageEnqueued {
                    queue_name: queue_name.clone(),
                    message_id: enqueue_result.id,
                    queue_depth,
                    timestamp: Utc::now(),
                });
            }
        }
    }

    result
}

async fn get_messages_with_events(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<GetMessagesRequest>,
) -> Result<Json<Vec<Message>>, StatusCode> {
    let result = get_messages(State(state.clone()), Path(queue_name.clone()), Json(req)).await;

    if let Ok(Json(ref messages)) = result {
        if !messages.is_empty() {
            let queues = state.queues.read().await;
            let queue_depth = queues.get(&queue_name).map(|q| q.size()).unwrap_or(0);
            drop(queues);

            state.event_broadcaster.broadcast(QueueEvent::MessagesDequeued {
                queue_name,
                count: messages.len(),
                queue_depth,
                timestamp: Utc::now(),
            });
        }
    }

    result
}

async fn delete_message_with_events(
    State(state): State<AppState>,
    Path((queue_name, receipt_handle)): Path<(String, uuid::Uuid)>,
) -> StatusCode {
    let result = delete_message(State(state.clone()), Path((queue_name.clone(), receipt_handle))).await;

    if result == StatusCode::NO_CONTENT {
        let queues = state.queues.read().await;
        let queue_depth = queues.get(&queue_name).map(|q| q.size()).unwrap_or(0);
        drop(queues);

        state.event_broadcaster.broadcast(QueueEvent::MessageDeleted {
            queue_name,
            receipt_handle,
            queue_depth,
            timestamp: Utc::now(),
        });
    }

    result
}

async fn batch_delete_with_events(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<BatchDeleteRequest>,
) -> Result<Json<BatchDeleteResponse>, StatusCode> {
    let result = batch_delete_messages(State(state.clone()), Path(queue_name.clone()), Json(req)).await;

    if let Ok(Json(ref response)) = result {
        let queues = state.queues.read().await;
        let queue_depth = queues.get(&queue_name).map(|q| q.size()).unwrap_or(0);
        drop(queues);

        state.event_broadcaster.broadcast(QueueEvent::BatchDeleted {
            queue_name,
            successful: response.successful,
            failed: response.failed,
            queue_depth,
            timestamp: Utc::now(),
        });
    }

    result
}

// Serve dashboard at root
async fn serve_dashboard() -> Response {
    match std::fs::read_to_string("static/dashboard.html") {
        Ok(content) => Response::builder()
            .header("Content-Type", "text/html")
            .body(content.into())
            .unwrap(),
        Err(_) => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("Dashboard not found. Make sure static/dashboard.html exists.".into())
            .unwrap(),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let storage_path = PathBuf::from("./queue_specs");
    let state = AppState::new(storage_path);

    // Check for authentication configuration
    let auth_validator = BasicAuthValidator::from_env();

    // Create static directory if it doesn't exist
    std::fs::create_dir_all("./static").ok();

    let app = Router::new()
        // Dashboard at root
        .route("/", get(serve_dashboard))
        .route("/dashboard", get(serve_dashboard))

        // Add OpenAPI JSON endpoint
        .route("/api-docs/openapi.json", get(|| async {
            Json(ApiDoc::openapi())
        }))

        // Health check and monitoring
        .route("/health", get(health_check))
        .route("/metrics", get(get_metrics))
        .route("/metrics/summary", get(get_metrics_summary))
        .route("/queues/:name/metrics", get(get_queue_metrics))

        // SSE endpoints for real-time updates
        .route("/events", get(sse_events))
        .route("/queues/:name/events", get(sse_queue_events))

        // Queue management (with event broadcasting)
        .route("/queues", post(create_queue_with_events))
        .route("/queues", get(list_queues))
        .route("/queues/:name", delete(delete_queue_with_events))
        .route("/queues/:name/settings", put(update_queue_settings))
        .route("/queues/:name/purge", post(purge_queue_with_events))
        .route("/queues/:name/cleanup", post(cleanup_expired_messages))

        // Message operations (with event broadcasting)
        .route("/queues/:name/messages", post(enqueue_message_with_events))
        .route("/queues/:name/messages/batch", post(enqueue_batch_with_events))
        .route("/queues/:name/messages/get", post(get_messages_with_events))
        .route("/queues/:name/messages/peek", post(peek_messages))
        .route("/queues/:name/messages/all", get(list_all_messages))
        .route("/queues/:name/messages/:receipt_handle", delete(delete_message_with_events))
        .route("/queues/:name/messages/batch-delete", post(batch_delete_with_events))

        // Queue details
        .route("/queues/:name/details", get(get_queue_details))

        // Time-series statistics for graphs
        .route("/queues/:name/stats/history", get(get_queue_history))
        .route("/stats/history", get(get_global_history))

        // Serve static files for frontend dashboard
        .nest_service("/static", ServeDir::new("static"))

        // Configure CORS to allow all origins, methods, and headers
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any)
                .expose_headers(Any)
                // Note: Cannot use allow_credentials(true) with wildcard origins/headers
                .max_age(Duration::from_secs(3600))
        )
        .with_state(state.clone());

    // Apply authentication middleware if configured
    let app = if let Some(validator) = auth_validator {
        info!("Basic authentication enabled for user: {}", validator.username);
        app.layer(middleware::from_fn_with_state(
            validator,
            basic_auth_middleware,
        ))
    } else {
        info!("Basic authentication disabled (set AUTH_USER and AUTH_PASSWORD to enable)");
        app
    };
    
    let listener = tokio::net::TcpListener::bind("0.0.0.0:4000")
        .await
        .unwrap();
    
    info!("RSQueue server running on http://0.0.0.0:4000");
    
    axum::serve(listener, app).await.unwrap();
}