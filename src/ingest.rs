use crate::error::LogQueryError;
use crate::indices::Indices;
use crate::metadata::Metadata;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader},
    sync::RwLock,
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
    pub offset: u64,
    pub log: LogMessage,
}

pub async fn receive_log_task(
    mut rx: tokio::sync::mpsc::Receiver<ParsedLog>,
    indices: Arc<RwLock<Indices>>,
) {
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
            let cleaned: String = word
            .to_lowercase()
            .chars()
            .filter(|c| c.is_alphanumeric())
            .collect();
            indices.words.entry(cleaned.clone()).or_default().insert(idx);
            
            let reversed: String = cleaned.chars().rev().collect();
            indices.rev_words.entry(reversed).or_default().insert(idx);
        }

        let ts = log.timestamp.timestamp();
        indices.timestamps.entry(ts).or_default().insert(idx);
    }
}

pub async fn read_file_task(
    tx: tokio::sync::mpsc::Sender<ParsedLog>,
    start_offset: u64,
    metadata: Arc<RwLock<Metadata>>,
    filepath: &str,
) -> Result<(), LogQueryError> {
    let mut file = File::open(filepath).await?;
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
                let log = ParsedLog {
                    offset: line_start_offset,
                    log: log_line,
                };

                {
                    let mut meta = metadata.write().await;
                    meta.last_offset = current_pos;
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
