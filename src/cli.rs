use crate::indices::Indices;
use crate::query::run_query;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader, stdin};
use tokio::sync::RwLock;

pub async fn cli_task(indices: Arc<RwLock<Indices>>) {
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

        if let Err(e) = run_query(input, &indices).await {
            println!("Query error: {}", e);
        }
    }
}
