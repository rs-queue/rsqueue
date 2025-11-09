use clap::{Parser, Subcommand};
use reqwest::blocking::Client;
use serde_json::json;
use std::process;

#[derive(Parser)]
#[command(name = "rsqueue-cli")]
#[command(about = "CLI tool for RSQueue operations", long_about = None)]
struct Cli {
    /// RSQueue server URL
    #[arg(short, long, default_value = "http://localhost:4000", env = "RSQUEUE_URL")]
    url: String,

    /// Basic auth username (optional)
    #[arg(short = 'u', long, env = "RSQUEUE_USER")]
    user: Option<String>,

    /// Basic auth password (optional)
    #[arg(short = 'p', long, env = "RSQUEUE_PASSWORD")]
    password: Option<String>,

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
    },

    /// List all queues
    List,

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
        } => create_queue(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            &name,
            visibility_timeout,
            dedup,
            dedup_window,
        ),

        Commands::List => list_queues(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
        ),

        Commands::Delete { name } => delete_queue(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            &name,
        ),

        Commands::Purge { name } => purge_queue(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            &name,
        ),

        Commands::Send {
            queue,
            content,
            ttl,
            delay,
        } => send_message(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            &queue,
            &content,
            ttl,
            delay,
        ),

        Commands::Receive { queue, count } => receive_messages(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
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
            &queue,
            &receipt_handle,
        ),

        Commands::Details { name } => get_details(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
            &name,
        ),

        Commands::Metrics { name } => get_metrics(
            &client,
            &cli.url,
            cli.user.as_deref(),
            cli.password.as_deref(),
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
            &queue,
            count,
            offset,
        ),
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
) -> reqwest::blocking::RequestBuilder {
    let mut req = client.request(method, url);

    if let (Some(u), Some(p)) = (user, password) {
        req = req.basic_auth(u, Some(p));
    }

    req
}

fn create_queue(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    name: &str,
    visibility_timeout: u64,
    dedup: bool,
    dedup_window: u64,
) -> Result<(), String> {
    let url = format!("{}/queues", base_url);
    let body = json!({
        "name": name,
        "visibility_timeout_seconds": visibility_timeout,
        "enable_deduplication": dedup,
        "deduplication_window_seconds": dedup_window,
    });

    let response = build_request(client, reqwest::Method::POST, &url, user, password)
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
) -> Result<(), String> {
    let url = format!("{}/queues", base_url);

    let response = build_request(client, reqwest::Method::GET, &url, user, password)
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

fn delete_queue(
    client: &Client,
    base_url: &str,
    user: Option<&str>,
    password: Option<&str>,
    name: &str,
) -> Result<(), String> {
    let url = format!("{}/queues/{}", base_url, name);

    let response = build_request(client, reqwest::Method::DELETE, &url, user, password)
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
    name: &str,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/purge", base_url, name);

    let response = build_request(client, reqwest::Method::POST, &url, user, password)
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
    queue: &str,
    content: &str,
    ttl: Option<u64>,
    delay: Option<u64>,
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

    let response = build_request(client, reqwest::Method::POST, &url, user, password)
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
    queue: &str,
    count: usize,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/messages/get", base_url, queue);
    let body = json!({
        "count": count,
    });

    let response = build_request(client, reqwest::Method::POST, &url, user, password)
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
    queue: &str,
    receipt_handle: &str,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/messages/{}", base_url, queue, receipt_handle);

    let response = build_request(client, reqwest::Method::DELETE, &url, user, password)
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
    name: &str,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/details", base_url, name);

    let response = build_request(client, reqwest::Method::GET, &url, user, password)
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
    name: &str,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/metrics", base_url, name);

    let response = build_request(client, reqwest::Method::GET, &url, user, password)
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
    queue: &str,
    count: usize,
    offset: usize,
) -> Result<(), String> {
    let url = format!("{}/queues/{}/messages/peek", base_url, queue);
    let body = json!({
        "count": count,
        "offset": offset,
    });

    let response = build_request(client, reqwest::Method::POST, &url, user, password)
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
