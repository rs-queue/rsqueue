use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{DateTime, Duration, Timelike, Utc};
use prometheus::{Histogram, IntCounter, IntGauge, Registry};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>, // TTL - message expires and is deleted at this time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_after: Option<DateTime<Utc>>, // Scheduled delivery - message won't be delivered before this time
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueueSpec {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub visibility_timeout_seconds: u64,
    pub enable_deduplication: bool,
    pub deduplication_window_seconds: u64, // How long to remember message hashes
}

// Time-series tracking for graphs
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TimeSeriesPoint {
    pub timestamp: DateTime<Utc>,
    pub enqueued: u64,
    pub dequeued: u64,
    pub deleted: u64,
    pub queue_depth: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueueTimeSeries {
    pub queue_name: String,
    pub data_points: Vec<TimeSeriesPoint>,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub granularity_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GlobalTimeSeries {
    pub data_points: Vec<TimeSeriesPoint>,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub granularity_seconds: u64,
}

// Internal structure for tracking events in time buckets
#[derive(Debug, Clone, Default)]
pub struct TimeBucketStats {
    pub enqueued: u64,
    pub dequeued: u64,
    pub deleted: u64,
    pub queue_depth_samples: Vec<usize>,
}

#[derive(Debug)]
pub struct TimeSeriesTracker {
    // Store stats per minute bucket (key is timestamp truncated to minute)
    pub buckets: BTreeMap<DateTime<Utc>, TimeBucketStats>,
    pub max_retention_hours: u64,
}

impl TimeSeriesTracker {
    pub fn new(max_retention_hours: u64) -> Self {
        Self {
            buckets: BTreeMap::new(),
            max_retention_hours,
        }
    }

    fn truncate_to_minute(dt: DateTime<Utc>) -> DateTime<Utc> {
        dt.date_naive()
            .and_hms_opt(dt.time().hour(), dt.time().minute(), 0)
            .unwrap()
            .and_utc()
    }

    pub fn record_enqueue(&mut self, queue_depth: usize) {
        let bucket_time = Self::truncate_to_minute(Utc::now());
        let stats = self.buckets.entry(bucket_time).or_default();
        stats.enqueued += 1;
        stats.queue_depth_samples.push(queue_depth);
        self.cleanup_old_buckets();
    }

    pub fn record_dequeue(&mut self, count: usize, queue_depth: usize) {
        let bucket_time = Self::truncate_to_minute(Utc::now());
        let stats = self.buckets.entry(bucket_time).or_default();
        stats.dequeued += count as u64;
        stats.queue_depth_samples.push(queue_depth);
        self.cleanup_old_buckets();
    }

    pub fn record_delete(&mut self, queue_depth: usize) {
        let bucket_time = Self::truncate_to_minute(Utc::now());
        let stats = self.buckets.entry(bucket_time).or_default();
        stats.deleted += 1;
        stats.queue_depth_samples.push(queue_depth);
        self.cleanup_old_buckets();
    }

    pub fn record_batch_delete(&mut self, count: usize, queue_depth: usize) {
        let bucket_time = Self::truncate_to_minute(Utc::now());
        let stats = self.buckets.entry(bucket_time).or_default();
        stats.deleted += count as u64;
        stats.queue_depth_samples.push(queue_depth);
        self.cleanup_old_buckets();
    }

    fn cleanup_old_buckets(&mut self) {
        let cutoff = Utc::now() - Duration::hours(self.max_retention_hours as i64);
        self.buckets.retain(|time, _| *time >= cutoff);
    }

    pub fn get_data_points(&self, from: DateTime<Utc>, to: DateTime<Utc>, granularity_seconds: u64) -> Vec<TimeSeriesPoint> {
        let mut result = Vec::new();
        let granularity = Duration::seconds(granularity_seconds as i64);

        let mut current = Self::truncate_to_minute(from);
        while current <= to {
            let mut point = TimeSeriesPoint {
                timestamp: current,
                enqueued: 0,
                dequeued: 0,
                deleted: 0,
                queue_depth: 0,
            };

            // Aggregate buckets within this granularity window
            let window_end = current + granularity;
            let mut depth_samples = Vec::new();

            for (_bucket_time, stats) in self.buckets.range(current..window_end) {
                point.enqueued += stats.enqueued;
                point.dequeued += stats.dequeued;
                point.deleted += stats.deleted;
                depth_samples.extend(&stats.queue_depth_samples);
            }

            // Use average queue depth for the window
            if !depth_samples.is_empty() {
                point.queue_depth = depth_samples.iter().sum::<usize>() / depth_samples.len();
            }

            result.push(point);
            current = window_end;
        }

        result
    }
}

#[derive(Debug)]
pub struct Queue {
    pub spec: QueueSpec,
    pub messages: VecDeque<Message>,
    pub in_flight: HashMap<Uuid, Message>, // receipt_handle -> Message
    pub dedup_hashes: HashMap<String, DateTime<Utc>>, // content_hash -> expiry time
    pub time_series: TimeSeriesTracker, // Track message events over time
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
            time_series: TimeSeriesTracker::new(24), // Keep 24 hours of history by default
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

    // Remove messages that have exceeded their TTL
    fn clean_expired_messages(&mut self) {
        let now = Utc::now();
        let initial_count = self.messages.len();

        // Remove expired messages from the main queue
        self.messages.retain(|msg| {
            if let Some(expires_at) = msg.expires_at {
                expires_at > now
            } else {
                true // Messages without TTL never expire
            }
        });

        // Remove expired messages from in-flight
        let expired_in_flight: Vec<Uuid> = self.in_flight
            .iter()
            .filter_map(|(handle, msg)| {
                if let Some(expires_at) = msg.expires_at {
                    if expires_at <= now {
                        Some(*handle)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        for handle in expired_in_flight {
            self.in_flight.remove(&handle);
        }

        // Track how many messages were removed
        let removed_count = initial_count - self.messages.len();
        if removed_count > 0 {
            info!("Removed {} expired messages from queue {}", removed_count, self.spec.name);
        }
    }

    pub fn enqueue(&mut self, content: String, ttl_seconds: Option<u64>, delay_seconds: Option<u64>) -> Result<Uuid, String> {
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

        // Calculate expiration time if TTL is specified
        let expires_at = ttl_seconds.map(|ttl| Utc::now() + Duration::seconds(ttl as i64));

        // Calculate delivery time if delay is specified
        let delivery_after = delay_seconds.map(|delay| Utc::now() + Duration::seconds(delay as i64));

        let message = Message {
            id: Uuid::new_v4(),
            content,
            content_hash,
            created_at: Utc::now(),
            receipt_handle: None,
            visible_after: None,
            expires_at,
            delivery_after,
        };
        let id = message.id;
        self.messages.push_back(message);
        MESSAGES_ENQUEUED_TOTAL.inc();
        self.time_series.record_enqueue(self.size());
        Ok(id)
    }

    pub fn enqueue_batch(&mut self, messages: Vec<BatchMessageRequest>) -> Vec<Result<Uuid, String>> {
        // Preallocate with exact capacity for better performance
        let mut results = Vec::with_capacity(messages.len());

        for msg in messages {
            results.push(self.enqueue(msg.content, msg.ttl_seconds, msg.delay_seconds));
        }

        results
    }

    pub fn dequeue(&mut self, count: usize) -> Vec<Message> {
        let _timer = OPERATION_DURATION.start_timer();
        let now = Utc::now();
        let visibility_timeout = Duration::seconds(self.spec.visibility_timeout_seconds as i64);

        // Clean expired messages and dedup hashes periodically
        self.clean_expired_messages();
        if self.spec.enable_deduplication {
            self.clean_expired_dedup_hashes();
        }

        // Atomically handle expired messages and dequeue new ones in a single operation
        // This ensures no other thread can interfere between checking expired and dequeuing

        // Step 1: Process expired messages atomically
        // Preallocate with estimated capacity for better performance
        let mut expired_messages = Vec::with_capacity(self.in_flight.len().min(100));
        let mut expired_handles = Vec::with_capacity(self.in_flight.len().min(100));

        for (handle, msg) in self.in_flight.iter() {
            if msg.visible_after.map(|va| va <= now).unwrap_or(false) {
                expired_handles.push(*handle);
            }
        }

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

        // Step 2: Dequeue requested messages atomically, skipping delayed messages
        // Preallocate with exact capacity for better performance
        let mut result = Vec::with_capacity(count);
        let mut messages_to_flight = Vec::with_capacity(count);
        let mut delayed_messages = Vec::with_capacity(count.min(10));

        // Store initial queue size to know how many messages we can check
        let initial_queue_size = self.messages.len();
        let mut checked = 0;

        // We need to check all messages to account for delayed ones
        while result.len() < count && checked < initial_queue_size {
            if let Some(mut msg) = self.messages.pop_front() {
                checked += 1;

                // Check if message is ready for delivery
                if let Some(delivery_after) = msg.delivery_after {
                    if delivery_after > now {
                        // Message is delayed, put it back for later
                        delayed_messages.push(msg);
                        continue;
                    }
                }

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

        // Put delayed messages back at the front of the queue
        for msg in delayed_messages.into_iter().rev() {
            self.messages.push_front(msg);
        }

        // Add all messages to in_flight in one atomic batch
        for (handle, msg) in messages_to_flight {
            self.in_flight.insert(handle, msg);
        }

        // Record dequeue event
        if !result.is_empty() {
            self.time_series.record_dequeue(result.len(), self.size());
        }

        result
    }

    pub fn delete_message(&mut self, receipt_handle: Uuid) -> bool {
        // When deleting a message, we keep its hash in dedup cache if dedup is enabled
        // This prevents the same message from being re-added during the dedup window
        let deleted = self.in_flight.remove(&receipt_handle).is_some();
        if deleted {
            MESSAGES_DELETED_TOTAL.inc();
            self.time_series.record_delete(self.size());
        }
        deleted
    }

    pub fn batch_delete_messages(&mut self, receipt_handles: Vec<Uuid>) -> Vec<DeleteResult> {
        // Preallocate with exact capacity for better performance
        let mut results = Vec::with_capacity(receipt_handles.len());
        let mut successful_count = 0;

        for handle in receipt_handles {
            // Don't call delete_message to avoid double-recording
            let success = self.in_flight.remove(&handle).is_some();
            if success {
                MESSAGES_DELETED_TOTAL.inc();
                successful_count += 1;
            }
            results.push(DeleteResult {
                receipt_handle: handle,
                success,
            });
        }

        // Record batch delete event once for all successful deletes
        if successful_count > 0 {
            self.time_series.record_batch_delete(successful_count, self.size());
        }

        results
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
                expires_at: msg.expires_at,
                delivery_after: msg.delivery_after,
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
                    expires_at: msg.expires_at,
                    delivery_after: msg.delivery_after,
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
                expires_at: msg.expires_at,
                delivery_after: msg.delivery_after,
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
                expires_at: msg.expires_at,
                delivery_after: msg.delivery_after,
            });
        }

        // Sort by creation time
        result.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        result
    }

    pub fn get_detailed_info(&self, queue_name: &str) -> QueueDetailInfo {
        let oldest_message_age_seconds = self.get_oldest_message_age_seconds();
        let average_message_age_seconds = self.get_average_message_age_seconds();

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
            oldest_message_age_seconds,
            average_message_age_seconds,
        }
    }

    fn get_oldest_message_age_seconds(&self) -> Option<i64> {
        let now = Utc::now();

        // Check both pending and in-flight messages
        let oldest_pending = self.messages.iter().map(|m| m.created_at).min();
        let oldest_in_flight = self.in_flight.values().map(|m| m.created_at).min();

        // Find the older of the two
        match (oldest_pending, oldest_in_flight) {
            (Some(p), Some(f)) => Some((now - p.min(f)).num_seconds()),
            (Some(p), None) => Some((now - p).num_seconds()),
            (None, Some(f)) => Some((now - f).num_seconds()),
            (None, None) => None,
        }
    }

    fn get_average_message_age_seconds(&self) -> Option<f64> {
        let now = Utc::now();
        let total_messages = self.size();

        if total_messages == 0 {
            return None;
        }

        let total_age: i64 = self.messages
            .iter()
            .chain(self.in_flight.values())
            .map(|m| (now - m.created_at).num_seconds())
            .sum();

        Some(total_age as f64 / total_messages as f64)
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
                                    time_series: TimeSeriesTracker::new(24),
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>, // Optional TTL in seconds - message will be deleted after this time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delay_seconds: Option<u64>, // Optional delay in seconds - message won't be delivered before this time
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BatchMessageRequest {
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delay_seconds: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BatchEnqueueRequest {
    pub messages: Vec<BatchMessageRequest>,
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
pub struct BatchDeleteRequest {
    pub receipt_handles: Vec<Uuid>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct BatchDeleteResponse {
    pub results: Vec<DeleteResult>,
    pub successful: usize,
    pub failed: usize,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeleteResult {
    pub receipt_handle: Uuid,
    pub success: bool,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_after: Option<DateTime<Utc>>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_message_age_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average_message_age_seconds: Option<f64>,
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

// Query parameters for time-series endpoints
#[derive(Debug, Deserialize, IntoParams)]
pub struct TimeSeriesQuery {
    /// Number of minutes to look back (default: 60)
    #[serde(default = "default_history_minutes")]
    pub minutes: u64,
    /// Granularity in seconds for data aggregation (default: 60 = 1 minute)
    #[serde(default = "default_granularity")]
    pub granularity_seconds: u64,
}

pub fn default_history_minutes() -> u64 {
    60 // Default to last hour
}

pub fn default_granularity() -> u64 {
    60 // Default to 1 minute buckets
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
        match queue.enqueue(req.content, req.ttl_seconds, req.delay_seconds) {
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
    path = "/queues/{name}/messages/batch-delete",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    request_body = BatchDeleteRequest,
    responses(
        (status = 200, description = "Batch delete results", body = BatchDeleteResponse),
        (status = 404, description = "Queue not found")
    )
)]
pub async fn batch_delete_messages(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<BatchDeleteRequest>,
) -> Result<Json<BatchDeleteResponse>, StatusCode> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();

    let mut queues = state.queues.write().await;

    if let Some(queue) = queues.get_mut(&queue_name) {
        let results = queue.batch_delete_messages(req.receipt_handles);
        let successful = results.iter().filter(|r| r.success).count();
        let failed = results.len() - successful;

        Ok(Json(BatchDeleteResponse {
            results,
            successful,
            failed,
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
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

// Time-series API handlers for graphs
#[utoipa::path(
    get,
    path = "/queues/{name}/stats/history",
    tag = "stats",
    params(
        ("name" = String, Path, description = "Queue name"),
        TimeSeriesQuery
    ),
    responses(
        (status = 200, description = "Time-series data for queue", body = QueueTimeSeries),
        (status = 404, description = "Queue not found")
    )
)]
pub async fn get_queue_history(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Query(params): Query<TimeSeriesQuery>,
) -> Result<Json<QueueTimeSeries>, StatusCode> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();

    let queues = state.queues.read().await;

    if let Some(queue) = queues.get(&queue_name) {
        let to = Utc::now();
        let from = to - Duration::minutes(params.minutes as i64);

        let data_points = queue.time_series.get_data_points(from, to, params.granularity_seconds);

        Ok(Json(QueueTimeSeries {
            queue_name: queue_name.clone(),
            data_points,
            from,
            to,
            granularity_seconds: params.granularity_seconds,
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[utoipa::path(
    get,
    path = "/stats/history",
    tag = "stats",
    params(TimeSeriesQuery),
    responses(
        (status = 200, description = "Global time-series data across all queues", body = GlobalTimeSeries)
    )
)]
pub async fn get_global_history(
    State(state): State<AppState>,
    Query(params): Query<TimeSeriesQuery>,
) -> Json<GlobalTimeSeries> {
    let _timer = HTTP_REQUEST_DURATION.start_timer();
    HTTP_REQUESTS_TOTAL.inc();

    let queues = state.queues.read().await;
    let to = Utc::now();
    let from = to - Duration::minutes(params.minutes as i64);
    let granularity = Duration::seconds(params.granularity_seconds as i64);

    // Collect all data points from all queues
    let mut aggregated: BTreeMap<DateTime<Utc>, TimeSeriesPoint> = BTreeMap::new();

    // Initialize time buckets
    let mut current = TimeSeriesTracker::truncate_to_minute(from);
    while current <= to {
        aggregated.insert(current, TimeSeriesPoint {
            timestamp: current,
            enqueued: 0,
            dequeued: 0,
            deleted: 0,
            queue_depth: 0,
        });
        current = current + granularity;
    }

    // Aggregate data from all queues
    for queue in queues.values() {
        let queue_points = queue.time_series.get_data_points(from, to, params.granularity_seconds);

        for point in queue_points {
            if let Some(agg_point) = aggregated.get_mut(&point.timestamp) {
                agg_point.enqueued += point.enqueued;
                agg_point.dequeued += point.dequeued;
                agg_point.deleted += point.deleted;
                agg_point.queue_depth += point.queue_depth;
            }
        }
    }

    let data_points: Vec<TimeSeriesPoint> = aggregated.into_values().collect();

    Json(GlobalTimeSeries {
        data_points,
        from,
        to,
        granularity_seconds: params.granularity_seconds,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;
    use tokio::time::sleep;

    #[test]
    fn test_message_ttl() {
        let mut queue = Queue::new(
            "test_queue".to_string(),
            30,
            false,
            300,
        );

        // Test message without TTL (should not expire)
        let id1 = queue.enqueue("Message without TTL".to_string(), None, None).unwrap();

        // Test message with TTL of 5 seconds
        let id2 = queue.enqueue("Message with 5s TTL".to_string(), Some(5), None).unwrap();

        // Test message with TTL of 1 hour
        let id3 = queue.enqueue("Message with 1hr TTL".to_string(), Some(3600), None).unwrap();

        // Check all messages are present initially
        assert_eq!(queue.messages.len(), 3);

        // Verify TTL is set correctly
        let msg_no_ttl = queue.messages.iter().find(|m| m.id == id1).unwrap();
        assert!(msg_no_ttl.expires_at.is_none());

        let msg_5s_ttl = queue.messages.iter().find(|m| m.id == id2).unwrap();
        assert!(msg_5s_ttl.expires_at.is_some());

        let msg_1hr_ttl = queue.messages.iter().find(|m| m.id == id3).unwrap();
        assert!(msg_1hr_ttl.expires_at.is_some());
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let mut queue = Queue::new(
            "test_queue".to_string(),
            30,
            false,
            300,
        );

        // Add a message with a very short TTL (1 second)
        let _expired_id = queue.enqueue("Should expire".to_string(), Some(1), None).unwrap();

        // Add a message without TTL
        let _persistent_id = queue.enqueue("Should persist".to_string(), None, None).unwrap();

        // Add a message with longer TTL
        let _long_ttl_id = queue.enqueue("Should also persist".to_string(), Some(60), None).unwrap();

        assert_eq!(queue.messages.len(), 3);

        // Sleep for 2 seconds to let the first message expire
        sleep(StdDuration::from_secs(2)).await;

        // Clean expired messages
        queue.clean_expired_messages();

        // Should have 2 messages left (the expired one should be removed)
        assert_eq!(queue.messages.len(), 2);

        // Verify the correct messages remain
        let remaining_contents: Vec<String> = queue.messages
            .iter()
            .map(|m| m.content.clone())
            .collect();

        assert!(remaining_contents.contains(&"Should persist".to_string()));
        assert!(remaining_contents.contains(&"Should also persist".to_string()));
        assert!(!remaining_contents.contains(&"Should expire".to_string()));
    }

    #[test]
    fn test_batch_enqueue_with_ttl() {
        let mut queue = Queue::new(
            "test_queue".to_string(),
            30,
            false,
            300,
        );

        let batch = vec![
            BatchMessageRequest {
                content: "Message 1".to_string(),
                ttl_seconds: Some(10),
                delay_seconds: None,
            },
            BatchMessageRequest {
                content: "Message 2".to_string(),
                ttl_seconds: None,
                delay_seconds: None,
            },
            BatchMessageRequest {
                content: "Message 3".to_string(),
                ttl_seconds: Some(3600),
                delay_seconds: None,
            },
        ];

        let results = queue.enqueue_batch(batch);

        // All messages should be enqueued successfully
        assert_eq!(results.len(), 3);
        for result in &results {
            assert!(result.is_ok());
        }

        // Verify TTL settings
        assert_eq!(queue.messages.len(), 3);

        let msg1 = queue.messages.iter().find(|m| m.content == "Message 1").unwrap();
        assert!(msg1.expires_at.is_some());

        let msg2 = queue.messages.iter().find(|m| m.content == "Message 2").unwrap();
        assert!(msg2.expires_at.is_none());

        let msg3 = queue.messages.iter().find(|m| m.content == "Message 3").unwrap();
        assert!(msg3.expires_at.is_some());
    }

    #[test]
    fn test_dequeue_cleans_expired_messages() {
        let mut queue = Queue::new(
            "test_queue".to_string(),
            30,
            false,
            300,
        );

        // Create a message that has already expired
        let mut expired_msg = Message {
            id: Uuid::new_v4(),
            content: "Already expired".to_string(),
            content_hash: Queue::compute_content_hash("Already expired"),
            created_at: Utc::now() - Duration::seconds(100),
            receipt_handle: None,
            visible_after: None,
            expires_at: Some(Utc::now() - Duration::seconds(10)), // Expired 10 seconds ago
            delivery_after: None,
        };

        // Create a valid message
        let valid_msg = Message {
            id: Uuid::new_v4(),
            content: "Valid message".to_string(),
            content_hash: Queue::compute_content_hash("Valid message"),
            created_at: Utc::now(),
            receipt_handle: None,
            visible_after: None,
            expires_at: Some(Utc::now() + Duration::seconds(100)), // Expires in 100 seconds
            delivery_after: None,
        };

        queue.messages.push_back(expired_msg.clone());
        queue.messages.push_back(valid_msg.clone());

        // Also test in-flight expired message
        let receipt = Uuid::new_v4();
        expired_msg.receipt_handle = Some(receipt);
        queue.in_flight.insert(receipt, expired_msg);

        assert_eq!(queue.messages.len(), 2);
        assert_eq!(queue.in_flight.len(), 1);

        // Dequeue should clean expired messages
        let dequeued = queue.dequeue(10);

        // Should only get the valid message
        assert_eq!(dequeued.len(), 1);
        assert_eq!(dequeued[0].content, "Valid message");

        // Expired message should be removed from in-flight
        assert_eq!(queue.in_flight.len(), 1); // The valid message is now in-flight
    }

    #[test]
    fn test_message_preview_includes_ttl() {
        let mut queue = Queue::new(
            "test_queue".to_string(),
            30,
            false,
            300,
        );

        let _id1 = queue.enqueue("Message with TTL".to_string(), Some(60), None).unwrap();
        let _id2 = queue.enqueue("Message without TTL".to_string(), None, None).unwrap();

        let previews = queue.peek_messages(10, 0);
        assert_eq!(previews.len(), 2);

        let preview_with_ttl = previews.iter().find(|p| p.content == "Message with TTL").unwrap();
        assert!(preview_with_ttl.expires_at.is_some());

        let preview_without_ttl = previews.iter().find(|p| p.content == "Message without TTL").unwrap();
        assert!(preview_without_ttl.expires_at.is_none());

        // Test list_all_messages also includes TTL
        let all_messages = queue.list_all_messages();
        assert_eq!(all_messages.len(), 2);

        let msg_with_ttl = all_messages.iter().find(|m| m.content == "Message with TTL").unwrap();
        assert!(msg_with_ttl.expires_at.is_some());
    }

    #[tokio::test]
    async fn test_delayed_messages() {
        let mut queue = Queue::new(
            "test_queue".to_string(),
            30,
            false,
            300,
        );

        // Add immediate message
        let _immediate_id = queue.enqueue("Immediate message".to_string(), None, None).unwrap();

        // Add delayed message (10 seconds)
        let _delayed_id = queue.enqueue("Delayed message".to_string(), None, Some(10)).unwrap();

        assert_eq!(queue.messages.len(), 2);

        // Try to dequeue - should only get immediate message
        let messages = queue.dequeue(10);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "Immediate message");

        // Delayed message should still be in queue
        assert_eq!(queue.messages.len(), 1);

        // Verify delayed message has delivery_after set
        let delayed_msg = queue.messages.front().unwrap();
        assert_eq!(delayed_msg.content, "Delayed message");
        assert!(delayed_msg.delivery_after.is_some());
    }

    #[test]
    fn test_batch_delete() {
        let mut queue = Queue::new(
            "test_queue".to_string(),
            30,
            false,
            300,
        );

        // Enqueue some messages
        let _id1 = queue.enqueue("Message 1".to_string(), None, None).unwrap();
        let _id2 = queue.enqueue("Message 2".to_string(), None, None).unwrap();
        let _id3 = queue.enqueue("Message 3".to_string(), None, None).unwrap();

        // Dequeue all messages
        let messages = queue.dequeue(3);
        assert_eq!(messages.len(), 3);

        // Collect receipt handles
        let handles: Vec<Uuid> = messages
            .iter()
            .map(|m| m.receipt_handle.unwrap())
            .collect();

        // Batch delete
        let results = queue.batch_delete_messages(handles);
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.success));

        // All messages should be deleted
        assert_eq!(queue.in_flight.len(), 0);
    }

    #[test]
    fn test_queue_statistics() {
        let mut queue = Queue::new(
            "test_queue".to_string(),
            30,
            false,
            300,
        );

        // Empty queue statistics
        let detail_info = queue.get_detailed_info("test_queue");
        assert!(detail_info.oldest_message_age_seconds.is_none());
        assert!(detail_info.average_message_age_seconds.is_none());

        // Add some messages
        queue.enqueue("Message 1".to_string(), None, None).unwrap();
        queue.enqueue("Message 2".to_string(), None, None).unwrap();
        queue.enqueue("Message 3".to_string(), None, None).unwrap();

        // Get statistics
        let detail_info = queue.get_detailed_info("test_queue");
        assert!(detail_info.oldest_message_age_seconds.is_some());
        assert!(detail_info.average_message_age_seconds.is_some());

        let oldest_age = detail_info.oldest_message_age_seconds.unwrap();
        let avg_age = detail_info.average_message_age_seconds.unwrap();

        // Age should be very small (just created)
        assert!(oldest_age < 2);
        assert!(avg_age < 2.0);

        // Verify statistics in detail info
        assert_eq!(detail_info.messages_pending, 3);
        assert_eq!(detail_info.messages_in_flight, 0);
        assert_eq!(detail_info.size, 3);
    }
}