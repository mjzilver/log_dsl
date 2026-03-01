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
use std::sync::Arc;
use tokio::{
    fs::create_dir_all,
    sync::{RwLock, mpsc},
};

#[tokio::main]
async fn main() -> Result<(), LogQueryError> {
    create_dir_all("./indices").await?;

    let indices = Arc::new(RwLock::new(Indices {
        levels: load_index_file("./indices/levels.idx").await?,
        words: load_index_file("./indices/words.idx").await?,
        timestamps: load_index_file("./indices/timestamps.idx").await?,
    }));

    let metadata_val = load_metadata().await?;
    let metadata = Arc::new(RwLock::new(metadata_val));

    let writer = tokio::spawn(write_periodically(
        Arc::clone(&indices),
        Arc::clone(&metadata),
    ));

    let (tx, rx) = mpsc::channel::<ParsedLog>(1024);

    let start_offset = {
        let m = metadata.read().await;
        m.last_offset
    };

    let reader = tokio::spawn(async move {
        if let Err(e) = read_file_task(tx, start_offset, Arc::clone(&metadata)).await {
            eprintln!("Reader task error: {}", e);
        }
    });

    let indices_for_receiver = Arc::clone(&indices);
    let indices_for_cli = Arc::clone(&indices);

    let receiver = tokio::spawn(async move {
        receive_log_task(rx, Arc::clone(&indices_for_receiver)).await;
    });

    let cli = tokio::spawn(async move {
        cli_task(Arc::clone(&indices_for_cli)).await;
    });

    let _ = tokio::join!(reader, receiver, writer, cli);

    Ok(())
}
