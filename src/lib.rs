use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{DateTime, Duration, Utc};
use prometheus::{Histogram, IntCounter, IntGauge, Registry};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

lazy_static::lazy_static! {
    static ref REGISTRY: Registry = Registry::new();
    
    // Queue operation metrics
    static ref MESSAGES_ENQUEUED_TOTAL: IntCounter = IntCounter::new(
        "rsqueue_messages_enqueued_total", 
        "Total number of messages enqueued"
    ).unwrap();
    
    static ref MESSAGES_DEQUEUED_TOTAL: IntCounter = IntCounter::new(
        "rsqueue_messages_dequeued_total", 
        "Total number of messages dequeued"
    ).unwrap();
    
    static ref MESSAGES_DELETED_TOTAL: IntCounter = IntCounter::new(
        "rsqueue_messages_deleted_total", 
        "Total number of messages deleted"
    ).unwrap();
    
    static ref DUPLICATE_MESSAGES_REJECTED_TOTAL: IntCounter = IntCounter::new(
        "rsqueue_duplicate_messages_rejected_total", 
        "Total number of duplicate messages rejected"
    ).unwrap();
    
    static ref MESSAGES_EXPIRED_TOTAL: IntCounter = IntCounter::new(
        "rsqueue_messages_expired_total", 
        "Total number of messages that expired and returned to queue"
    ).unwrap();
    
    // Queue management metrics
    static ref QUEUES_CREATED_TOTAL: IntCounter = IntCounter::new(
        "rsqueue_queues_created_total", 
        "Total number of queues created"
    ).unwrap();
    
    static ref QUEUES_DELETED_TOTAL: IntCounter = IntCounter::new(
        "rsqueue_queues_deleted_total", 
        "Total number of queues deleted"
    ).unwrap();
    
    static ref QUEUES_PURGED_TOTAL: IntCounter = IntCounter::new(
        "rsqueue_queues_purged_total", 
        "Total number of queues purged"
    ).unwrap();
    
    // Current state metrics
    static ref ACTIVE_QUEUES: IntGauge = IntGauge::new(
        "rsqueue_active_queues", 
        "Number of active queues"
    ).unwrap();
    
    static ref TOTAL_MESSAGES_PENDING: IntGauge = IntGauge::new(
        "rsqueue_total_messages_pending", 
        "Total number of pending messages across all queues"
    ).unwrap();
    
    static ref TOTAL_MESSAGES_IN_FLIGHT: IntGauge = IntGauge::new(
        "rsqueue_total_messages_in_flight", 
        "Total number of in-flight messages across all queues"
    ).unwrap();
    
    // Performance metrics
    static ref OPERATION_DURATION: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "rsqueue_operation_duration_seconds",
            "Duration of queue operations in seconds"
        ).buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
    ).unwrap();
    
    // HTTP metrics
    static ref HTTP_REQUESTS_TOTAL: IntCounter = IntCounter::new(
        "rsqueue_http_requests_total", 
        "Total number of HTTP requests"
    ).unwrap();
    
    static ref HTTP_REQUEST_DURATION: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "rsqueue_http_request_duration_seconds",
            "Duration of HTTP requests in seconds"
        ).buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
    ).unwrap();
}

// Data structures
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Message {
    pub id: Uuid,
    pub content: String,
    pub content_hash: String, // SHA-256 hash of content for deduplication
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt_handle: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visible_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueueSpec {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub visibility_timeout_seconds: u64,
    pub enable_deduplication: bool,
    pub deduplication_window_seconds: u64, // How long to remember message hashes
}

#[derive(Debug)]
pub struct Queue {
    pub spec: QueueSpec,
    pub messages: VecDeque<Message>,
    pub in_flight: HashMap<Uuid, Message>, // receipt_handle -> Message
    pub dedup_hashes: HashMap<String, DateTime<Utc>>, // content_hash -> expiry time
}

// Additional atomic helper methods for Queue
impl Queue {
    pub fn new(name: String, visibility_timeout_seconds: u64, enable_deduplication: bool, deduplication_window_seconds: u64) -> Self {
        Self {
            spec: QueueSpec {
                name,
                created_at: Utc::now(),
                visibility_timeout_seconds,
                enable_deduplication,
                deduplication_window_seconds,
            },
            messages: VecDeque::new(),
            in_flight: HashMap::new(),
            dedup_hashes: HashMap::new(),
        }
    }

    pub fn compute_content_hash(content: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        let hash = hasher.finalize();
        BASE64.encode(hash)
    }

    fn clean_expired_dedup_hashes(&mut self) {
        let now = Utc::now();
        self.dedup_hashes.retain(|_, expiry| *expiry > now);
    }

    pub fn enqueue(&mut self, content: String) -> Result<Uuid, String> {
        let _timer = OPERATION_DURATION.start_timer();
        let content_hash = Self::compute_content_hash(&content);
        
        // Check for deduplication if enabled
        if self.spec.enable_deduplication {
            self.clean_expired_dedup_hashes();
            
            if self.dedup_hashes.contains_key(&content_hash) {
                DUPLICATE_MESSAGES_REJECTED_TOTAL.inc();
                return Err(format!("Duplicate message detected (hash: {})", &content_hash[..8]));
            }
            
            // Add hash with expiry time
            let expiry = Utc::now() + Duration::seconds(self.spec.deduplication_window_seconds as i64);
            self.dedup_hashes.insert(content_hash.clone(), expiry);
        }
        
        let message = Message {
            id: Uuid::new_v4(),
            content,
            content_hash,
            created_at: Utc::now(),
            receipt_handle: None,
            visible_after: None,
        };
        let id = message.id;
        self.messages.push_back(message);
        MESSAGES_ENQUEUED_TOTAL.inc();
        Ok(id)
    }

    pub fn enqueue_batch(&mut self, contents: Vec<String>) -> Vec<Result<Uuid, String>> {
        contents.into_iter()
            .map(|content| self.enqueue(content))
            .collect()
    }

    pub fn dequeue(&mut self, count: usize) -> Vec<Message> {
        let _timer = OPERATION_DURATION.start_timer();
        let now = Utc::now();
        let visibility_timeout = Duration::seconds(self.spec.visibility_timeout_seconds as i64);
        
        // Clean expired dedup hashes periodically
        if self.spec.enable_deduplication {
            self.clean_expired_dedup_hashes();
        }
        
        // Atomically handle expired messages and dequeue new ones in a single operation
        // This ensures no other thread can interfere between checking expired and dequeuing
        
        // Step 1: Process expired messages atomically
        let mut expired_messages = Vec::new();
        let expired_handles: Vec<Uuid> = self
            .in_flight
            .iter()
            .filter_map(|(handle, msg)| {
                if msg.visible_after.map(|va| va <= now).unwrap_or(false) {
                    Some(*handle)
                } else {
                    None
                }
            })
            .collect();

        for handle in expired_handles {
            if let Some(mut msg) = self.in_flight.remove(&handle) {
                msg.receipt_handle = None;
                msg.visible_after = None;
                expired_messages.push(msg);
                MESSAGES_EXPIRED_TOTAL.inc();
            }
        }
        
        // Add expired messages back to the queue
        for msg in expired_messages {
            self.messages.push_back(msg);
        }

        // Step 2: Dequeue requested messages atomically
        let mut result = Vec::new();
        let mut messages_to_flight = Vec::new();
        
        for _ in 0..count {
            if let Some(mut msg) = self.messages.pop_front() {
                let receipt_handle = Uuid::new_v4();
                msg.receipt_handle = Some(receipt_handle);
                msg.visible_after = Some(now + visibility_timeout);
                messages_to_flight.push((receipt_handle, msg.clone()));
                result.push(msg);
                MESSAGES_DEQUEUED_TOTAL.inc();
            } else {
                break;
            }
        }
        
        // Add all messages to in_flight in one atomic batch
        for (handle, msg) in messages_to_flight {
            self.in_flight.insert(handle, msg);
        }
        
        result
    }

    pub fn delete_message(&mut self, receipt_handle: Uuid) -> bool {
        // When deleting a message, we keep its hash in dedup cache if dedup is enabled
        // This prevents the same message from being re-added during the dedup window
        let deleted = self.in_flight.remove(&receipt_handle).is_some();
        if deleted {
            MESSAGES_DELETED_TOTAL.inc();
        }
        deleted
    }

    pub fn purge(&mut self) {
        self.messages.clear();
        self.in_flight.clear();
        // Keep dedup hashes to prevent re-adding recently purged messages
    }

    pub fn size(&self) -> usize {
        self.messages.len() + self.in_flight.len()
    }

    pub fn total_bytes(&self) -> usize {
        // Calculate total bytes from all messages in queue (pending + in-flight)
        let pending_bytes: usize = self.messages.iter()
            .map(|msg| msg.content.len())
            .sum();

        let in_flight_bytes: usize = self.in_flight.values()
            .map(|msg| msg.content.len())
            .sum();

        pending_bytes + in_flight_bytes
    }

    // Thread-safe visibility check for monitoring
    pub fn get_visible_count(&self) -> usize {
        let now = Utc::now();
        let expired_count = self.in_flight.values()
            .filter(|msg| msg.visible_after.map(|va| va <= now).unwrap_or(false))
            .count();

        self.messages.len() + expired_count
    }

    pub fn get_dedup_cache_size(&self) -> usize {
        self.dedup_hashes.len()
    }

    pub fn peek_messages(&self, count: usize, offset: usize) -> Vec<MessagePreview> {
        let _now = Utc::now();
        let mut result = Vec::new();
        
        // Get pending messages
        let pending_messages: Vec<MessagePreview> = self.messages
            .iter()
            .skip(offset)
            .take(count)
            .map(|msg| MessagePreview {
                id: msg.id,
                content: msg.content.clone(),
                created_at: msg.created_at,
                status: MessageStatus::Pending,
                visible_after: None,
            })
            .collect();
        
        result.extend(pending_messages);
        
        // If we need more messages, get from in-flight
        if result.len() < count {
            let remaining = count - result.len();
            let in_flight_offset = if offset > self.messages.len() { 
                offset - self.messages.len() 
            } else { 
                0 
            };
            
            let in_flight_messages: Vec<MessagePreview> = self.in_flight
                .values()
                .skip(in_flight_offset)
                .take(remaining)
                .map(|msg| MessagePreview {
                    id: msg.id,
                    content: msg.content.clone(),
                    created_at: msg.created_at,
                    status: MessageStatus::InFlight,
                    visible_after: msg.visible_after,
                })
                .collect();
            
            result.extend(in_flight_messages);
        }
        
        result
    }

    pub fn list_all_messages(&self) -> Vec<MessagePreview> {
        let mut result = Vec::new();
        
        // Add all pending messages
        for msg in &self.messages {
            result.push(MessagePreview {
                id: msg.id,
                content: msg.content.clone(),
                created_at: msg.created_at,
                status: MessageStatus::Pending,
                visible_after: None,
            });
        }
        
        // Add all in-flight messages
        for msg in self.in_flight.values() {
            result.push(MessagePreview {
                id: msg.id,
                content: msg.content.clone(),
                created_at: msg.created_at,
                status: MessageStatus::InFlight,
                visible_after: msg.visible_after,
            });
        }
        
        // Sort by creation time
        result.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        result
    }

    pub fn get_detailed_info(&self, queue_name: &str) -> QueueDetailInfo {
        QueueDetailInfo {
            name: queue_name.to_string(),
            size: self.size(),
            total_bytes: self.total_bytes(),
            created_at: self.spec.created_at,
            visibility_timeout_seconds: self.spec.visibility_timeout_seconds,
            enable_deduplication: self.spec.enable_deduplication,
            deduplication_window_seconds: self.spec.deduplication_window_seconds,
            dedup_cache_size: self.get_dedup_cache_size(),
            messages_pending: self.messages.len(),
            messages_in_flight: self.in_flight.len(),
            visible_messages: self.get_visible_count(),
            recent_messages: self.peek_messages(5, 0), // Show recent 5 messages
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub queues: Arc<RwLock<HashMap<String, Queue>>>,
    pub storage_path: PathBuf,
}

impl AppState {
    pub fn new(storage_path: PathBuf) -> Self {
        // Initialize metrics registry
        Self::initialize_metrics();
        
        fs::create_dir_all(&storage_path).ok();
        let state = Self {
            queues: Arc::new(RwLock::new(HashMap::new())),
            storage_path,
        };
        
        // Load existing queue specs
        if let Ok(entries) = fs::read_dir(&state.storage_path) {
            for entry in entries.flatten() {
                if let Some(ext) = entry.path().extension() {
                    if ext == "json" {
                        if let Ok(content) = fs::read_to_string(entry.path()) {
                            if let Ok(spec) = serde_json::from_str::<QueueSpec>(&content) {
                                let queue = Queue {
                                    spec: spec.clone(),
                                    messages: VecDeque::new(),
                                    in_flight: HashMap::new(),
                                    dedup_hashes: HashMap::new(),
                                };
                                
                                // For tests, just insert directly without spawning
                                #[cfg(test)]
                                {
                                    use std::sync::Arc;
                                    let queues = Arc::clone(&state.queues);
                                    let name = spec.name.clone();
                                    tokio::task::block_in_place(move || {
                                        tokio::runtime::Handle::current().block_on(async move {
                                            let mut queues = queues.write().await;
                                            queues.insert(name, queue);
                                            ACTIVE_QUEUES.inc();
                                        });
                                    });
                                }
                                
                                #[cfg(not(test))]
                                {
                                    tokio::spawn({
                                        let queues = state.queues.clone();
                                        let name = spec.name.clone();
                                        async move {
                                            let mut queues = queues.write().await;
                                            queues.insert(name, queue);
                                            ACTIVE_QUEUES.inc();
                                        }
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
        
        state
    }
    
    fn initialize_metrics() {
        // Register all metrics with the global registry, ignoring AlreadyReg errors
        let _ = REGISTRY.register(Box::new(MESSAGES_ENQUEUED_TOTAL.clone()));
        let _ = REGISTRY.register(Box::new(MESSAGES_DEQUEUED_TOTAL.clone()));
        let _ = REGISTRY.register(Box::new(MESSAGES_DELETED_TOTAL.clone()));
        let _ = REGISTRY.register(Box::new(DUPLICATE_MESSAGES_REJECTED_TOTAL.clone()));
        let _ = REGISTRY.register(Box::new(MESSAGES_EXPIRED_TOTAL.clone()));
        let _ = REGISTRY.register(Box::new(QUEUES_CREATED_TOTAL.clone()));
        let _ = REGISTRY.register(Box::new(QUEUES_DELETED_TOTAL.clone()));
        let _ = REGISTRY.register(Box::new(QUEUES_PURGED_TOTAL.clone()));
        let _ = REGISTRY.register(Box::new(ACTIVE_QUEUES.clone()));
        let _ = REGISTRY.register(Box::new(TOTAL_MESSAGES_PENDING.clone()));
        let _ = REGISTRY.register(Box::new(TOTAL_MESSAGES_IN_FLIGHT.clone()));
        let _ = REGISTRY.register(Box::new(OPERATION_DURATION.clone()));
        let _ = REGISTRY.register(Box::new(HTTP_REQUESTS_TOTAL.clone()));
        let _ = REGISTRY.register(Box::new(HTTP_REQUEST_DURATION.clone()));
    }
    
    pub async fn update_global_metrics(&self) {
        let queues = self.queues.read().await;
        let mut total_pending = 0;
        let mut total_in_flight = 0;
        
        for queue in queues.values() {
            total_pending += queue.messages.len();
            total_in_flight += queue.in_flight.len();
        }
        
        ACTIVE_QUEUES.set(queues.len() as i64);
        TOTAL_MESSAGES_PENDING.set(total_pending as i64);
        TOTAL_MESSAGES_IN_FLIGHT.set(total_in_flight as i64);
    }

    pub async fn save_queue_spec(&self, spec: &QueueSpec) -> Result<(), std::io::Error> {
        let path = self.storage_path.join(format!("{}.json", spec.name));
        let content = serde_json::to_string_pretty(spec)?;
        fs::write(path, content)?;
        Ok(())
    }

    pub async fn delete_queue_spec(&self, name: &str) -> Result<(), std::io::Error> {
        let path = self.storage_path.join(format!("{}.json", name));
        fs::remove_file(path)?;
        Ok(())
    }
}

// Request/Response types
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateQueueRequest {
    pub name: String,
    #[serde(default = "default_visibility_timeout")]
    pub visibility_timeout_seconds: u64,
    #[serde(default)]
    pub enable_deduplication: bool,
    #[serde(default = "default_deduplication_window")]
    pub deduplication_window_seconds: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateQueueRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility_timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enable_deduplication: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deduplication_window_seconds: Option<u64>,
}

pub fn default_visibility_timeout() -> u64 {
    120 // 2 minutes default
}

pub fn default_deduplication_window() -> u64 {
    300 // 5 minutes default
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct EnqueueRequest {
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BatchEnqueueRequest {
    pub messages: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct EnqueueResponse {
    pub id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BatchEnqueueResponse {
    pub results: Vec<EnqueueResponse>,
    pub successful: usize,
    pub failed: usize,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GetMessagesRequest {
    #[serde(default = "default_count")]
    pub count: usize,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PeekMessagesRequest {
    #[serde(default = "default_count")]
    pub count: usize,
    #[serde(default)]
    pub offset: usize,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct MessagePreview {
    pub id: uuid::Uuid,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub status: MessageStatus,
    pub visible_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub enum MessageStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "in_flight")]
    InFlight,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct QueueDetailInfo {
    pub name: String,
    pub size: usize,
    pub total_bytes: usize,
    pub created_at: DateTime<Utc>,
    pub visibility_timeout_seconds: u64,
    pub enable_deduplication: bool,
    pub deduplication_window_seconds: u64,
    pub dedup_cache_size: usize,
    pub messages_pending: usize,
    pub messages_in_flight: usize,
    pub visible_messages: usize,
    pub recent_messages: Vec<MessagePreview>,
}

pub fn default_count() -> usize {
    1
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct QueueInfo {
    pub name: String,
    pub size: usize,
    pub total_bytes: usize,
    pub created_at: DateTime<Utc>,
    pub visibility_timeout_seconds: u64,
    pub enable_deduplication: bool,
    pub deduplication_window_seconds: u64,
    pub dedup_cache_size: usize,
}

// Handlers
#[utoipa::path(
    post,
    path = "/queues",
    tag = "queues",
    request_body = CreateQueueRequest,
    responses(
        (status = 201, description = "Queue created successfully", body = QueueSpec),
        (status = 409, description = "Queue already exists"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn create_queue(
    State(state): State<AppState>,
    Json(req): Json<CreateQueueRequest>,
) -> Result<Json<QueueSpec>, StatusCode> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();
    
    let mut queues = state.queues.write().await;
    
    if queues.contains_key(&req.name) {
        return Err(StatusCode::CONFLICT);
    }
    
    let queue = Queue::new(
        req.name.clone(), 
        req.visibility_timeout_seconds,
        req.enable_deduplication,
        req.deduplication_window_seconds
    );
    let spec = queue.spec.clone();
    
    if let Err(e) = state.save_queue_spec(&spec).await {
        error!("Failed to save queue spec: {}", e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    
    queues.insert(req.name, queue);
    QUEUES_CREATED_TOTAL.inc();
    ACTIVE_QUEUES.inc();
    info!("Created queue: {} (dedup: {})", spec.name, spec.enable_deduplication);
    
    Ok(Json(spec))
}

#[utoipa::path(
    put,
    path = "/queues/{name}/settings",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    request_body = UpdateQueueRequest,
    responses(
        (status = 200, description = "Queue settings updated successfully", body = QueueSpec),
        (status = 404, description = "Queue not found"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn update_queue_settings(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<UpdateQueueRequest>,
) -> Result<Json<QueueSpec>, StatusCode> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();

    let mut queues = state.queues.write().await;

    if let Some(queue) = queues.get_mut(&queue_name) {
        let mut updated = false;

        if let Some(visibility_timeout) = req.visibility_timeout_seconds {
            queue.spec.visibility_timeout_seconds = visibility_timeout;
            updated = true;
        }

        if let Some(enable_dedup) = req.enable_deduplication {
            if enable_dedup != queue.spec.enable_deduplication {
                queue.spec.enable_deduplication = enable_dedup;
                if !enable_dedup {
                    queue.dedup_hashes.clear();
                }
                updated = true;
            }
        }

        if let Some(dedup_window) = req.deduplication_window_seconds {
            queue.spec.deduplication_window_seconds = dedup_window;
            updated = true;
        }

        if updated {
            let spec = queue.spec.clone();
            if let Err(e) = state.save_queue_spec(&spec).await {
                error!("Failed to save updated queue spec: {}", e);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }

            info!("Updated queue settings: {} (dedup: {})", spec.name, spec.enable_deduplication);
            Ok(Json(spec))
        } else {
            let spec = queue.spec.clone();
            Ok(Json(spec))
        }
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[utoipa::path(
    get,
    path = "/queues",
    tag = "queues",
    responses(
        (status = 200, description = "List of all queues", body = [QueueInfo])
    )
)]
pub async fn list_queues(State(state): State<AppState>) -> Json<Vec<QueueInfo>> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();
    
    let queues = state.queues.read().await;
    let info: Vec<QueueInfo> = queues
        .iter()
        .map(|(name, queue)| QueueInfo {
            name: name.clone(),
            size: queue.size(),
            total_bytes: queue.total_bytes(),
            created_at: queue.spec.created_at,
            visibility_timeout_seconds: queue.spec.visibility_timeout_seconds,
            enable_deduplication: queue.spec.enable_deduplication,
            deduplication_window_seconds: queue.spec.deduplication_window_seconds,
            dedup_cache_size: queue.get_dedup_cache_size(),
        })
        .collect();
    
    Json(info)
}

#[utoipa::path(
    delete,
    path = "/queues/{name}",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    responses(
        (status = 204, description = "Queue deleted successfully"),
        (status = 404, description = "Queue not found")
    )
)]
pub async fn delete_queue(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
) -> StatusCode {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();
    
    let mut queues = state.queues.write().await;
    
    if queues.remove(&queue_name).is_some() {
        if let Err(e) = state.delete_queue_spec(&queue_name).await {
            error!("Failed to delete queue spec file: {}", e);
        }
        QUEUES_DELETED_TOTAL.inc();
        ACTIVE_QUEUES.dec();
        info!("Deleted queue: {}", queue_name);
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

#[utoipa::path(
    post,
    path = "/queues/{name}/messages",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    request_body = EnqueueRequest,
    responses(
        (status = 200, description = "Message enqueued successfully", body = EnqueueResponse),
        (status = 404, description = "Queue not found")
    )
)]
pub async fn enqueue_message(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<EnqueueRequest>,
) -> Result<Json<EnqueueResponse>, StatusCode> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();
    
    let mut queues = state.queues.write().await;
    
    if let Some(queue) = queues.get_mut(&queue_name) {
        match queue.enqueue(req.content) {
            Ok(id) => Ok(Json(EnqueueResponse { id, error: None })),
            Err(e) => {
                warn!("Failed to enqueue message: {}", e);
                Ok(Json(EnqueueResponse { 
                    id: Uuid::nil(), 
                    error: Some(e) 
                }))
            }
        }
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[utoipa::path(
    post,
    path = "/queues/{name}/messages/batch",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    request_body = BatchEnqueueRequest,
    responses(
        (status = 200, description = "Batch messages enqueued", body = BatchEnqueueResponse),
        (status = 404, description = "Queue not found")
    )
)]
pub async fn enqueue_batch(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<BatchEnqueueRequest>,
) -> Result<Json<BatchEnqueueResponse>, StatusCode> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();
    
    let mut queues = state.queues.write().await;
    
    if let Some(queue) = queues.get_mut(&queue_name) {
        let results: Vec<EnqueueResponse> = queue
            .enqueue_batch(req.messages)
            .into_iter()
            .map(|result| match result {
                Ok(id) => EnqueueResponse { id, error: None },
                Err(e) => EnqueueResponse { 
                    id: Uuid::nil(), 
                    error: Some(e) 
                }
            })
            .collect();
        
        let successful = results.iter().filter(|r| r.error.is_none()).count();
        let failed = results.len() - successful;
        
        Ok(Json(BatchEnqueueResponse { 
            results, 
            successful, 
            failed 
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[utoipa::path(
    post,
    path = "/queues/{name}/messages/get",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    request_body = GetMessagesRequest,
    responses(
        (status = 200, description = "Retrieved messages", body = [Message]),
        (status = 404, description = "Queue not found")
    )
)]
pub async fn get_messages(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<GetMessagesRequest>,
) -> Result<Json<Vec<Message>>, StatusCode> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();
    
    // Use a single write lock for the entire operation to ensure atomicity
    let mut queues = state.queues.write().await;
    
    if let Some(queue) = queues.get_mut(&queue_name) {
        // The dequeue operation is now fully atomic under the write lock
        let messages = queue.dequeue(req.count);
        Ok(Json(messages))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[utoipa::path(
    delete,
    path = "/queues/{name}/messages/{receipt_handle}",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name"),
        ("receipt_handle" = String, Path, description = "Receipt handle of the message")
    ),
    responses(
        (status = 204, description = "Message deleted successfully"),
        (status = 404, description = "Queue or message not found")
    )
)]
pub async fn delete_message(
    State(state): State<AppState>,
    Path((queue_name, receipt_handle)): Path<(String, Uuid)>,
) -> StatusCode {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();
    
    let mut queues = state.queues.write().await;
    
    if let Some(queue) = queues.get_mut(&queue_name) {
        if queue.delete_message(receipt_handle) {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::NOT_FOUND
        }
    } else {
        StatusCode::NOT_FOUND
    }
}

#[utoipa::path(
    post,
    path = "/queues/{name}/purge",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    responses(
        (status = 204, description = "Queue purged successfully"),
        (status = 404, description = "Queue not found")
    )
)]
pub async fn purge_queue(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
) -> StatusCode {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();
    
    let mut queues = state.queues.write().await;
    
    if let Some(queue) = queues.get_mut(&queue_name) {
        queue.purge();
        QUEUES_PURGED_TOTAL.inc();
        info!("Purged queue: {}", queue_name);
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

// Metrics accessor functions
pub fn get_metrics_registry() -> &'static Registry {
    &REGISTRY
}

pub fn get_messages_enqueued_total() -> u64 {
    MESSAGES_ENQUEUED_TOTAL.get()
}

pub fn get_messages_dequeued_total() -> u64 {
    MESSAGES_DEQUEUED_TOTAL.get()
}

pub fn get_messages_deleted_total() -> u64 {
    MESSAGES_DELETED_TOTAL.get()
}

pub fn get_duplicate_messages_rejected_total() -> u64 {
    DUPLICATE_MESSAGES_REJECTED_TOTAL.get()
}

pub fn get_messages_expired_total() -> u64 {
    MESSAGES_EXPIRED_TOTAL.get()
}

pub fn get_queues_created_total() -> u64 {
    QUEUES_CREATED_TOTAL.get()
}

pub fn get_queues_deleted_total() -> u64 {
    QUEUES_DELETED_TOTAL.get()
}

pub fn get_queues_purged_total() -> u64 {
    QUEUES_PURGED_TOTAL.get()
}

pub fn get_active_queues() -> i64 {
    ACTIVE_QUEUES.get()
}

pub fn get_total_messages_pending() -> i64 {
    TOTAL_MESSAGES_PENDING.get()
}

pub fn get_total_messages_in_flight() -> i64 {
    TOTAL_MESSAGES_IN_FLIGHT.get()
}

pub fn get_http_requests_total() -> u64 {
    HTTP_REQUESTS_TOTAL.get()
}

// New API handlers for peeking at messages

#[utoipa::path(
    post,
    path = "/queues/{name}/messages/peek",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    request_body = PeekMessagesRequest,
    responses(
        (status = 200, description = "Retrieved messages preview", body = [MessagePreview]),
        (status = 404, description = "Queue not found")
    )
)]
pub async fn peek_messages(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<PeekMessagesRequest>,
) -> Result<Json<Vec<MessagePreview>>, StatusCode> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();
    
    let queues = state.queues.read().await;
    
    if let Some(queue) = queues.get(&queue_name) {
        let messages = queue.peek_messages(req.count, req.offset);
        Ok(Json(messages))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[utoipa::path(
    get,
    path = "/queues/{name}/details",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    responses(
        (status = 200, description = "Queue details with message preview", body = QueueDetailInfo),
        (status = 404, description = "Queue not found")
    )
)]
pub async fn get_queue_details(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
) -> Result<Json<QueueDetailInfo>, StatusCode> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();
    
    let queues = state.queues.read().await;
    
    if let Some(queue) = queues.get(&queue_name) {
        let details = queue.get_detailed_info(&queue_name);
        Ok(Json(details))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[utoipa::path(
    get,
    path = "/queues/{name}/messages/all",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    responses(
        (status = 200, description = "All messages in queue", body = [MessagePreview]),
        (status = 404, description = "Queue not found")
    )
)]
pub async fn list_all_messages(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
) -> Result<Json<Vec<MessagePreview>>, StatusCode> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();
    
    let queues = state.queues.read().await;
    
    if let Some(queue) = queues.get(&queue_name) {
        let messages = queue.list_all_messages();
        Ok(Json(messages))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}