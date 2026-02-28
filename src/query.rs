use crate::{
    error::LogQueryError,
    indices::Indices,
    metadata::{Metadata, NEXT_ID},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::{
    borrow::Borrow,
    collections::{BTreeSet, HashMap},
    hash::Hash,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader, stdin},
    sync::{
        RwLock,
        mpsc::{Receiver, Sender},
    },
    time,
};

use std::io::SeekFrom;

#[derive(Deserialize, Debug)]
pub struct LogMessage {
    pub level: String,
    pub message: String,
    pub timestamp: DateTime<Utc>,
}

pub struct ParsedLog {
    pub id: u64,
    pub offset: u64,
    pub log: LogMessage,
}

impl std::fmt::Display for LogMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}: {}", self.timestamp, self.level, self.message)
    }
}

pub async fn receive_log_task(mut rx: Receiver<ParsedLog>, indices: Arc<RwLock<Indices>>) {
    while let Some(parsed_log) = rx.recv().await {
        let log = &parsed_log.log;
        let idx = parsed_log.offset;
        let mut indices = indices.write().await;

        indices
            .levels
            .entry(log.level.clone())
            .or_default()
            .insert(idx);

        for word in log.message.split_whitespace() {
            indices
                .words
                .entry(word.to_lowercase())
                .or_default()
                .insert(idx);
        }

        let ts = log.timestamp.timestamp();
        indices.timestamps.entry(ts).or_default().insert(idx);
    }
}

pub async fn read_file_task(
    tx: Sender<ParsedLog>,
    start_offset: u64,
    metadata: Arc<RwLock<Metadata>>,
) -> Result<(), LogQueryError> {
    let mut file = File::open("./bot.log").await?;
    file.seek(SeekFrom::Start(start_offset)).await?;
    let mut buffer = Vec::new();
    let mut current_pos = start_offset;
    let mut chunk = vec![0u8; 1024];

    loop {
        let n = file.read(&mut chunk).await?;

        if n == 0 {
            // prevent busy waiting
            time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        buffer.extend_from_slice(&chunk[..n]);

        while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
            let line_bytes = buffer.drain(..=pos).collect::<Vec<_>>();

            let line_len = line_bytes.len() as u64;
            let line_start_offset = current_pos;
            current_pos += line_len;

            if let Ok(line_str) = String::from_utf8(line_bytes)
                && let Ok(log_line) = serde_json::from_str::<LogMessage>(line_str.trim())
            {
                let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

                let log = ParsedLog {
                    id,
                    offset: line_start_offset,
                    log: log_line,
                };

                {
                    let mut meta = metadata.write().await;
                    meta.last_offset = current_pos;
                    meta.last_id = id;
                }

                let _ = tx.send(log).await;
            }
        }
    }
}

pub async fn find_logs_by_offsets(offsets: &BTreeSet<u64>) -> Result<Vec<String>, LogQueryError> {
    let mut res = Vec::new();
    let mut iter = offsets.iter();
    let mut current_target = match iter.next() {
        Some(&v) => v,
        None => return Ok(res),
    };

    let file = File::open("./bot.log").await?;
    let mut reader = BufReader::new(file);

    let mut current_offset = 0u64;
    let mut line = Vec::new();

    while reader.read_until(b'\n', &mut line).await? > 0 {
        let line_start = current_offset;
        let line_end = current_offset + line.len() as u64;

        while current_target >= line_start && current_target < line_end {
            if let Ok(line_str) = String::from_utf8(line.clone()) {
                res.push(line_str.trim().to_string());
            }
            if let Some(&next) = iter.next() {
                current_target = next;
            } else {
                return Ok(res);
            }
        }

        current_offset = line_end;
        line.clear();
    }

    Ok(res)
}

pub async fn query<K, Q>(indices: &HashMap<K, BTreeSet<u64>>, needle: &Q)
where
    K: Eq + Hash + Borrow<Q>,
    Q: Eq + Hash + ?Sized,
{
    if let Some(offsets) = indices.get(needle) {
        match find_logs_by_offsets(offsets).await {
            Ok(logs) => {
                let mut output = String::new();
                for log in logs {
                    if let Ok(log_line) = serde_json::from_str::<LogMessage>(log.trim()) {
                        output.push_str(&format!("{}\n", log_line));
                    }
                    // output.push_str(&format!("{}\n", log));
                }
                print!("{}", output);
            }
            Err(e) => {
                println!("Error retrieving logs: {}", e);
            }
        }
    } else {
        println!("No results");
    }
}

pub async fn cli_task(indices: Arc<RwLock<Indices>>) {
    let mut reader = BufReader::new(stdin());
    let mut line = String::new();

    println!("Log query system ready.");
    println!("Example: level=warn or word=timeout");

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

        if input.starts_with("level=") {
            let level = input.trim_start_matches("level=");
            let guard = indices.read().await;

            query(&guard.levels, level).await;
        } else if input.starts_with("word=") {
            let word = input.trim_start_matches("word=");
            let guard = indices.read().await;

            query(&guard.words, word).await;
        }
    }
}
