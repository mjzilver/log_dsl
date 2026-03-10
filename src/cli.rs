use crate::indices::Indices;
use crate::metadata::Metadata;
use crate::query::run_query;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader, stdin};
use tokio::sync::RwLock;

pub async fn cli_task(indices: Arc<RwLock<Indices>>, metadata: Arc<RwLock<Metadata>>) {
    let mut reader = BufReader::new(stdin());
    let mut line = String::new();

    println!("Log query system ready.");
    println!("Example: level=warn OR word=timeout");

    loop {
        line.clear();

        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("CLI read error: {}", e);
                break;
            }
        }

        let input = line.trim();

        if input == "dump_all" {
            for word in indices.read().await.words.keys() {
                println!("{}", word);
            }
        } else if let Err(e) = run_query(input, &indices, &metadata).await {
            println!("Query error: {}", e);
        }
    }
}
