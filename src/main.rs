use axum::{
    response::{Json, Response},
    routing::{delete, get, post, put},
    Router,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use prometheus::{TextEncoder, Encoder};
use rsqueue::*;
use serde::Serialize;
use std::{path::PathBuf, time::SystemTime};
use tower_http::cors::CorsLayer;
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
    visible_messages: usize,
    dedup_cache_size: usize,
    created_at: DateTime<Utc>,
    visibility_timeout_seconds: u64,
    enable_deduplication: bool,
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
        peek_messages,
        get_queue_details,
        list_all_messages
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
        BatchEnqueueRequest,
        EnqueueResponse,
        BatchEnqueueResponse,
        GetMessagesRequest,
        QueueInfo,
        PeekMessagesRequest,
        MessagePreview,
        MessageStatus,
        QueueDetailInfo
    )),
    tags(
        (name = "health", description = "Health check and monitoring endpoints"),
        (name = "queues", description = "Queue management operations"),
        (name = "messages", description = "Message operations")
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
        Ok(Json(QueueMetrics {
            name: queue_name,
            messages_pending: queue.messages.len(),
            messages_in_flight: queue.in_flight.len(),
            total_size: queue.size(),
            visible_messages: queue.get_visible_count(),
            dedup_cache_size: queue.get_dedup_cache_size(),
            created_at: queue.spec.created_at,
            visibility_timeout_seconds: queue.spec.visibility_timeout_seconds,
            enable_deduplication: queue.spec.enable_deduplication,
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    
    let storage_path = PathBuf::from("./queue_specs");
    let state = AppState::new(storage_path);
    
    let app = Router::new()
        // Add OpenAPI JSON endpoint
        .route("/api-docs/openapi.json", get(|| async {
            Json(ApiDoc::openapi())
        }))
        
        // Health check and monitoring
        .route("/health", get(health_check))
        .route("/metrics", get(get_metrics))
        .route("/metrics/summary", get(get_metrics_summary))
        .route("/queues/:name/metrics", get(get_queue_metrics))
        
        // Queue management
        .route("/queues", post(create_queue))
        .route("/queues", get(list_queues))
        .route("/queues/:name", delete(delete_queue))
        .route("/queues/:name/settings", put(update_queue_settings))
        .route("/queues/:name/purge", post(purge_queue))
        
        // Message operations
        .route("/queues/:name/messages", post(enqueue_message))
        .route("/queues/:name/messages/batch", post(enqueue_batch))
        .route("/queues/:name/messages/get", post(get_messages))
        .route("/queues/:name/messages/peek", post(peek_messages))
        .route("/queues/:name/messages/all", get(list_all_messages))
        .route("/queues/:name/messages/:receipt_handle", delete(delete_message))
        
        // Queue details
        .route("/queues/:name/details", get(get_queue_details))
        
        .layer(CorsLayer::permissive())
        .with_state(state);
    
    let listener = tokio::net::TcpListener::bind("0.0.0.0:4000")
        .await
        .unwrap();
    
    info!("RSQueue server running on http://0.0.0.0:4000");
    
    axum::serve(listener, app).await.unwrap();
}