mod cli;
mod error;
mod indices;
mod ingest;
mod metadata;
mod parser;
mod query;

use crate::error::LogQueryError;
use crate::{
    cli::cli_task,
    indices::{Indices, load_index_file, write_periodically},
    ingest::{ParsedLog, read_file_task, receive_log_task},
    metadata::load_metadata,
};
use std::env;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::Notify;
use tokio::{
    fs::create_dir_all,
    sync::{RwLock, mpsc},
};

use sha2::{Digest, Sha256};
use tokio::fs;

async fn compute_file_id(path: &str) -> Result<String, LogQueryError> {
    let bytes = fs::read(path).await?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

#[tokio::main]
async fn main() -> Result<(), LogQueryError> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Warning: Please provide a file path as an argument");
        return Err(LogQueryError::FileNotFound(
            "No file path provided".to_string(),
        ));
    }
    let file_path = args[1].clone();
    if !fs::try_exists(&file_path).await? {
        return Err(LogQueryError::FileNotFound(file_path.to_string()));
    }

    let file_id = compute_file_id(&file_path).await?;
    let dir = format!("./indices/{}", file_id);
    create_dir_all("./indices").await?;
    create_dir_all(&dir).await?;

    let indices = Arc::new(RwLock::new(Indices {
        levels: load_index_file(&format!("{}/levels.idx", dir)).await?,
        words: load_index_file(&format!("{}/words.idx", dir)).await?,
        rev_words: load_index_file(&format!("{}/rev_words.idx", dir)).await?,
        timestamps: load_index_file(&format!("{}/timestamps.idx", dir)).await?,
        dirty: AtomicBool::new(false),
        notify: Arc::new(Notify::new()),
    }));

    let metadata_val =
        load_metadata(&format!("{}/metadata.json", dir), &file_path, &file_id).await?;
    let metadata = Arc::new(RwLock::new(metadata_val));

    let writer = tokio::spawn(write_periodically(
        Arc::clone(&indices),
        Arc::clone(&metadata),
        dir.clone(),
    ));

    let (tx, rx) = mpsc::channel::<ParsedLog>(1024);

    let start_offset = {
        let m = metadata.read().await;
        m.last_offset
    };

    let reader_metadata = Arc::clone(&metadata);
    let reader = tokio::spawn(async move {
        if let Err(e) = read_file_task(tx, start_offset, reader_metadata, &file_path).await {
            eprintln!("Reader task error: {}", e);
        }
    });

    let indices_for_receiver = Arc::clone(&indices);
    let indices_for_cli = Arc::clone(&indices);

    let receiver = tokio::spawn(async move {
        receive_log_task(rx, Arc::clone(&indices_for_receiver)).await;
    });

    let cli_metadata = Arc::clone(&metadata);
    let cli = tokio::spawn(async move {
        cli_task(Arc::clone(&indices_for_cli), cli_metadata).await;
    });

    let _ = tokio::join!(reader, receiver, writer, cli);

    Ok(())
}
