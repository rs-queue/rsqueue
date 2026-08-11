use clap::{Parser, Subcommand};
use reqwest::blocking::Client;
use serde_json::json;
use std::io::{Read as _, Write as _};
use std::process;
use tungstenite::connect;

#[derive(Parser)]
#[command(name = "rsqueue-cli")]
#[command(about = "CLI tool for RSQueue operations", long_about = None)]
struct Cli {
    /// RSQueue server URL
    #[arg(long, default_value = "http://localhost:4000", env = "RSQUEUE_URL")]
    url: String,

    /// Basic auth username (optional)
    #[arg(short = 'u', long, env = "RSQUEUE_USER")]
    user: Option<String>,

    /// Basic auth password (optional)
    #[arg(short = 'p', long, env = "RSQUEUE_PASSWORD")]
    password: Option<String>,

    /// API Key for authentication (optional)
    #[arg(short = 'k', long, env = "RSQUEUE_API_KEY")]
    api_key: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new queue
    Create {
        /// Name of the queue
        name: String,

        /// Visibility timeout in seconds
        #[arg(short, long, default_value = "120")]
        visibility_timeout: u64,

        /// Enable message deduplication
        #[arg(short, long)]
        dedup: bool,

        /// Deduplication window in seconds
        #[arg(short = 'w', long, default_value = "300")]
        dedup_window: u64,

        /// Enable in-memory message compression
        #[arg(long)]
        compress: bool,

        /// Dead letter queue name
        #[arg(long)]
        dlq: Option<String>,

        /// Max receive count before moving to DLQ
        #[arg(long)]
        max_receive_count: Option<u32>,

        /// Max messages (pending + in-flight) to hold before dropping the oldest
        #[arg(long)]
        max_size: Option<usize>,
    },

    /// List all queues
    List,

    /// Set (or clear) the size cap on an existing queue
    SetLimit {
        /// Name of the queue
        name: String,

        /// Max messages to hold; 0 removes the cap
        max_size: usize,
    },

    /// Delete a queue
    Delete {
        /// Name of the queue
        name: String,
    },

    /// Purge all messages from a queue
    Purge {
        /// Name of the queue
        name: String,
    },

    /// Send a message to a queue
    Send {
        /// Name of the queue
        queue: String,

        /// Message content
        content: String,

        /// Message TTL in seconds (optional)
        #[arg(short, long)]
        ttl: Option<u64>,

        /// Message delay in seconds (optional)
        #[arg(short, long)]
        delay: Option<u64>,

        /// Message priority 0-9 (higher = higher priority, default 0)
        #[arg(short = 'P', long)]
        priority: Option<u8>,
    },

    /// Receive messages from a queue
    Receive {
        /// Name of the queue
        queue: String,

        /// Number of messages to receive
        #[arg(short, long, default_value = "1")]
        count: usize,
    },

    /// Delete a message from a queue
    DeleteMessage {
        /// Name of the queue
        queue: String,

        /// Receipt handle of the message
        receipt_handle: String,
    },

    /// Get queue details
    Details {
        /// Name of the queue
        name: String,
    },

    /// Get queue metrics
    Metrics {
        /// Name of the queue
        name: String,
    },

    /// Peek at messages without removing them
    Peek {
        /// Name of the queue
        queue: String,

        /// Number of messages to peek at
        #[arg(short, long, default_value = "10")]
        count: usize,

        /// Offset to start from
        #[arg(short, long, default_value = "0")]
        offset: usize,
    },

    /// Listen to real-time events via WebSocket
    Listen {
        /// Subscribe to a specific queue (omit for all events)
        #[arg(short, long)]
        queue: Option<String>,
    },

    /// Consume messages via WebSocket with interactive ack/nack
    WsConsume {
        /// Name of the queue to consume from
        queue: String,

        /// Number of messages per consume request
        #[arg(short, long, default_value = "1")]
        count: usize,

        /// Automatically ack messages after printing
        #[arg(long)]
        auto_ack: bool,
    },
}

fn main() {
    let cli = Cli::parse();
    let client = Client::new();

    let result = match cli.command {
        Commands::Create {
            name,
            visibility_timeout,
            dedup,
            dedup_window,
            compress,
            dlq,
            max_receive_count,
            max_size,
        } => create_queue(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            cli.api_key.as_deref(),
            &name,
            visibility_timeout,
            dedup,
            dedup_window,
            compress,
            dlq.as_deref(),
            max_receive_count,
            max_size,
        ),

        Commands::List => list_queues(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            cli.api_key.as_deref(),
        ),

        Commands::SetLimit { name, max_size } => set_queue_limit(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            cli.api_key.as_deref(),
            &name,
            max_size,
        ),

        Commands::Delete { name } => delete_queue(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            cli.api_key.as_deref(),
            &name,
        ),

        Commands::Purge { name } => purge_queue(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            cli.api_key.as_deref(),
            &name,
        ),

        Commands::Send {
            queue,
            content,
            ttl,
            delay,
            priority,
        } => send_message(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            cli.api_key.as_deref(),
            &queue,
            &content,
            ttl,
            delay,
            priority,
        ),

        Commands::Receive { queue, count } => receive_messages(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            cli.api_key.as_deref(),
            &queue,
            count,
        ),

        Commands::DeleteMessage {
            queue,
            receipt_handle,
        } => delete_message(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            cli.api_key.as_deref(),
            &queue,
            &receipt_handle,
        ),

        Commands::Details { name } => get_details(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            cli.api_key.as_deref(),
            &name,
        ),

        Commands::Metrics { name } => get_metrics(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            cli.api_key.as_deref(),
            &name,
        ),

        Commands::Peek {
            queue,
            count,
            offset,
        } => peek_messages(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            cli.api_key.as_deref(),
            &queue,
            count,
            offset,
        ),

        Commands::Listen { queue } => ws_listen(&cli.url, queue.as_deref()),

        Commands::WsConsume {
            queue,
            count,
            auto_ack,
        } => ws_consume(&cli.url, &queue, count, auto_ack),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}

fn build_request(
    client: &Client,
    method: reqwest::Method,
    url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
) -> reqwest::blocking::RequestBuilder {
    let mut req = client.request(method, url);

    if let (Some(u), Some(p)) = (user, password) {
        req = req.basic_auth(u, Some(p));
    }

    if let Some(key) = api_key {
        req = req.header("X-Api-Key", key);
    }

    req
}

fn create_queue(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
    name: &str,
    visibility_timeout: u64,
    dedup: bool,
    dedup_window: u64,
    compress: bool,
    dlq: Option<&str>,
    max_receive_count: Option<u32>,
    max_size: Option<usize>,
) -> Result<(), String> {
    let url = format!("{}/queues", base_url);
    let mut body = json!({
        "name": name,
        "visibility_timeout_seconds": visibility_timeout,
        "enable_deduplication": dedup,
        "deduplication_window_seconds": dedup_window,
        "enable_compression": compress,
    });

    if let Some(dlq_name) = dlq {
        body["dead_letter_queue"] = json!(dlq_name);
    }
    if let Some(count) = max_receive_count {
        body["max_receive_count"] = json!(count);
    }
    if let Some(size) = max_size {
        body["max_queue_size"] = json!(size);
    }

    let response = build_request(client, reqwest::Method::POST, &url, user, password, api_key)
        .json(&body)
        .send()
        .map_err(|e| format!("Request failed: {}", e))?;

    if response.status().is_success() {
        let queue: serde_json::Value = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;
        println!("Queue created successfully:");
        println!("{}", serde_json::to_string_pretty(&queue).unwrap());
        Ok(())
    } else {
        Err(format!(
            "Failed to create queue: HTTP {}",
            response.status()
        ))
    }
}

fn list_queues(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
) -> Result<(), String> {
    let url = format!("{}/queues", base_url);

    let response = build_request(client, reqwest::Method::GET, &url, user, password, api_key)
        .send()
        .map_err(|e| format!("Request failed: {}", e))?;

    if response.status().is_success() {
        let queues: serde_json::Value = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;
        println!("{}", serde_json::to_string_pretty(&queues).unwrap());
        Ok(())
    } else {
        Err(format!("Failed to list queues: HTTP {}", response.status()))
    }
}

fn set_queue_limit(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
    name: &str,
    max_size: usize,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/settings", base_url, name);
    let body = json!({ "max_queue_size": max_size });

    let response = build_request(client, reqwest::Method::PUT, &url, user, password, api_key)
        .json(&body)
        .send()
        .map_err(|e| format!("Request failed: {}", e))?;

    if response.status().is_success() {
        let spec: serde_json::Value = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;
        if max_size == 0 {
            println!("Size cap removed from queue '{}'", name);
        } else {
            println!(
                "Queue '{}' capped at {} messages (oldest are dropped when full)",
                name, max_size
            );
        }
        println!("{}", serde_json::to_string_pretty(&spec).unwrap());
        Ok(())
    } else {
        Err(format!(
            "Failed to set queue limit: HTTP {}",
            response.status()
        ))
    }
}

fn delete_queue(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
    name: &str,
) -> Result<(), String> {
    let url = format!("{}/queues/{}", base_url, name);

    let response = build_request(client, reqwest::Method::DELETE, &url, user, password, api_key)
        .send()
        .map_err(|e| format!("Request failed: {}", e))?;

    if response.status().is_success() {
        println!("Queue '{}' deleted successfully", name);
        Ok(())
    } else {
        Err(format!("Failed to delete queue: HTTP {}", response.status()))
    }
}

fn purge_queue(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
    name: &str,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/purge", base_url, name);

    let response = build_request(client, reqwest::Method::POST, &url, user, password, api_key)
        .send()
        .map_err(|e| format!("Request failed: {}", e))?;

    if response.status().is_success() {
        println!("Queue '{}' purged successfully", name);
        Ok(())
    } else {
        Err(format!("Failed to purge queue: HTTP {}", response.status()))
    }
}

fn send_message(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
    queue: &str,
    content: &str,
    ttl: Option<u64>,
    delay: Option<u64>,
    priority: Option<u8>,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/messages", base_url, queue);
    let mut body = json!({
        "content": content,
    });

    if let Some(t) = ttl {
        body["ttl_seconds"] = json!(t);
    }

    if let Some(d) = delay {
        body["delay_seconds"] = json!(d);
    }

    if let Some(p) = priority {
        body["priority"] = json!(p);
    }

    let response = build_request(client, reqwest::Method::POST, &url, user, password, api_key)
        .json(&body)
        .send()
        .map_err(|e| format!("Request failed: {}", e))?;

    if response.status().is_success() {
        let result: serde_json::Value = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;
        println!("Message sent:");
        println!("{}", serde_json::to_string_pretty(&result).unwrap());
        Ok(())
    } else {
        Err(format!("Failed to send message: HTTP {}", response.status()))
    }
}

fn receive_messages(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
    queue: &str,
    count: usize,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/messages/get", base_url, queue);
    let body = json!({
        "count": count,
    });

    let response = build_request(client, reqwest::Method::POST, &url, user, password, api_key)
        .json(&body)
        .send()
        .map_err(|e| format!("Request failed: {}", e))?;

    if response.status().is_success() {
        let messages: serde_json::Value = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;
        println!("{}", serde_json::to_string_pretty(&messages).unwrap());
        Ok(())
    } else {
        Err(format!(
            "Failed to receive messages: HTTP {}",
            response.status()
        ))
    }
}

fn delete_message(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
    queue: &str,
    receipt_handle: &str,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/messages/{}", base_url, queue, receipt_handle);

    let response = build_request(client, reqwest::Method::DELETE, &url, user, password, api_key)
        .send()
        .map_err(|e| format!("Request failed: {}", e))?;

    if response.status().is_success() {
        println!("Message deleted successfully");
        Ok(())
    } else {
        Err(format!(
            "Failed to delete message: HTTP {}",
            response.status()
        ))
    }
}

fn get_details(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
    name: &str,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/details", base_url, name);

    let response = build_request(client, reqwest::Method::GET, &url, user, password, api_key)
        .send()
        .map_err(|e| format!("Request failed: {}", e))?;

    if response.status().is_success() {
        let details: serde_json::Value = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;
        println!("{}", serde_json::to_string_pretty(&details).unwrap());
        Ok(())
    } else {
        Err(format!("Failed to get details: HTTP {}", response.status()))
    }
}

fn get_metrics(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
    name: &str,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/metrics", base_url, name);

    let response = build_request(client, reqwest::Method::GET, &url, user, password, api_key)
        .send()
        .map_err(|e| format!("Request failed: {}", e))?;

    if response.status().is_success() {
        let metrics: serde_json::Value = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;
        println!("{}", serde_json::to_string_pretty(&metrics).unwrap());
        Ok(())
    } else {
        Err(format!("Failed to get metrics: HTTP {}", response.status()))
    }
}

fn peek_messages(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    api_key: Option<&str>,
    queue: &str,
    count: usize,
    offset: usize,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/messages/peek", base_url, queue);
    let body = json!({
        "count": count,
        "offset": offset,
    });

    let response = build_request(client, reqwest::Method::POST, &url, user, password, api_key)
        .json(&body)
        .send()
        .map_err(|e| format!("Request failed: {}", e))?;

    if response.status().is_success() {
        let messages: serde_json::Value = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;
        println!("{}", serde_json::to_string_pretty(&messages).unwrap());
        Ok(())
    } else {
        Err(format!(
            "Failed to peek messages: HTTP {}",
            response.status()
        ))
    }
}

fn http_to_ws_url(http_url: &str) -> String {
    http_url
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1)
}

fn ws_listen(base_url: &str, queue: Option<&str>) -> Result<(), String> {
    let ws_base = http_to_ws_url(base_url);
    let ws_url = match queue {
        Some(q) => format!("{}/queues/{}/ws", ws_base, q),
        None => format!("{}/ws", ws_base),
    };

    let (mut socket, _response) =
        connect(&ws_url).map_err(|e| format!("WebSocket connection failed: {}", e))?;

    // If global endpoint and no queue specified, subscribe to all
    if queue.is_none() {
        let sub_msg = json!({"type": "subscribe_all"});
        socket
            .send(tungstenite::Message::Text(sub_msg.to_string().into()))
            .map_err(|e| format!("Failed to send subscribe: {}", e))?;
    }

    eprintln!("Connected. Listening for events... (Ctrl+C to stop)");

    loop {
        match socket.read() {
            Ok(tungstenite::Message::Text(text)) => {
                // Pretty-print if valid JSON, otherwise raw
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                    println!("{}", serde_json::to_string_pretty(&val).unwrap());
                } else {
                    println!("{}", text);
                }
            }
            Ok(tungstenite::Message::Close(_)) => {
                eprintln!("Server closed connection");
                break;
            }
            Err(e) => {
                return Err(format!("WebSocket error: {}", e));
            }
            _ => {} // ignore binary, ping/pong
        }
    }

    Ok(())
}

fn ws_consume(base_url: &str, queue: &str, count: usize, auto_ack: bool) -> Result<(), String> {
    let ws_base = http_to_ws_url(base_url);
    let ws_url = format!("{}/queues/{}/ws", ws_base, queue);

    let (mut socket, _response) =
        connect(&ws_url).map_err(|e| format!("WebSocket connection failed: {}", e))?;

    // Read the connected message
    if let Ok(tungstenite::Message::Text(text)) = socket.read() {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
            eprintln!(
                "Connected to server v{}",
                val.get("server_version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
            );
        }
    }

    // Read the auto-subscribed message
    let _ = socket.read();

    loop {
        // Send consume request
        let consume_msg = json!({
            "type": "consume",
            "queue_name": queue,
            "count": count,
        });
        socket
            .send(tungstenite::Message::Text(consume_msg.to_string().into()))
            .map_err(|e| format!("Failed to send consume: {}", e))?;

        // Read response
        match socket.read() {
            Ok(tungstenite::Message::Text(text)) => {
                let val: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| format!("Invalid JSON: {}", e))?;

                let msg_type = val.get("type").and_then(|t| t.as_str()).unwrap_or("");

                if msg_type == "messages" {
                    let messages = val.get("messages").and_then(|m| m.as_array());
                    if let Some(msgs) = messages {
                        if msgs.is_empty() {
                            eprintln!("No messages available. Waiting 2s...");
                            std::thread::sleep(std::time::Duration::from_secs(2));
                            continue;
                        }

                        for msg in msgs {
                            println!("{}", serde_json::to_string_pretty(msg).unwrap());

                            if let Some(receipt_handle) = msg.get("receipt_handle").and_then(|r| r.as_str()) {
                                if auto_ack {
                                    let ack_msg = json!({
                                        "type": "ack",
                                        "queue_name": queue,
                                        "receipt_handle": receipt_handle,
                                    });
                                    socket
                                        .send(tungstenite::Message::Text(ack_msg.to_string().into()))
                                        .map_err(|e| format!("Failed to send ack: {}", e))?;
                                    // Read ack response
                                    let _ = socket.read();
                                    eprintln!("  -> auto-acked {}", receipt_handle);
                                } else {
                                    eprint!("  [a]ck / [n]ack / [s]kip? ");
                                    std::io::stderr().flush().ok();
                                    let mut input = [0u8; 1];
                                    if std::io::stdin().read(&mut input).is_ok() {
                                        // Consume rest of line
                                        let mut rest = String::new();
                                        let _ = std::io::stdin().read_line(&mut rest);

                                        match input[0] {
                                            b'a' | b'A' => {
                                                let ack_msg = json!({
                                                    "type": "ack",
                                                    "queue_name": queue,
                                                    "receipt_handle": receipt_handle,
                                                });
                                                socket
                                                    .send(tungstenite::Message::Text(ack_msg.to_string().into()))
                                                    .map_err(|e| format!("Failed to send ack: {}", e))?;
                                                let _ = socket.read();
                                                eprintln!("  -> acked");
                                            }
                                            b'n' | b'N' => {
                                                let nack_msg = json!({
                                                    "type": "nack",
                                                    "queue_name": queue,
                                                    "receipt_handle": receipt_handle,
                                                });
                                                socket
                                                    .send(tungstenite::Message::Text(nack_msg.to_string().into()))
                                                    .map_err(|e| format!("Failed to send nack: {}", e))?;
                                                let _ = socket.read();
                                                eprintln!("  -> nacked (returned to queue)");
                                            }
                                            _ => {
                                                eprintln!("  -> skipped (will return after visibility timeout)");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else if msg_type == "error" {
                    let err_msg = val.get("message").and_then(|m| m.as_str()).unwrap_or("unknown error");
                    eprintln!("Error: {}", err_msg);
                } else if msg_type == "pong" {
                    // heartbeat, ignore and retry consume
                    continue;
                }
            }
            Ok(tungstenite::Message::Close(_)) => {
                eprintln!("Server closed connection");
                break;
            }
            Err(e) => {
                return Err(format!("WebSocket error: {}", e));
            }
            _ => {}
        }
    }

    Ok(())
}
