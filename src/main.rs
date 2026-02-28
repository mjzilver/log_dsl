use std::{
    borrow::Borrow,
    collections::HashMap,
    fmt::{self, Display, Formatter},
    hash::Hash,
    io::{SeekFrom, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, stdin},
    sync::{
        RwLock,
        mpsc::{self, Receiver, Sender},
    },
};
use zstd::{Decoder, Encoder};

#[derive(Deserialize)]
pub struct LogMessage {
    level: String,
    message: String,
    timestamp: DateTime<Utc>,
}

pub struct ParsedLog {
    pub id: u64,
    pub offset: u64,
    pub log: LogMessage,
}

pub struct Indices {
    pub levels: HashMap<String, Vec<u64>>,
    pub words: HashMap<String, Vec<u64>>,
    // Unix timestamp
    pub timestamps: HashMap<i64, Vec<u64>>,
}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    last_offset: u64,
    last_id: u64,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

static METADATA_FILE: &str = "./indices/metadata.json";
static LEVEL_INDICES_FILE: &str = "./indices/levels.idx";
static WORD_INDICES_FILE: &str = "./indices/words.idx";
static TIMESTAMP_INDICES_FILE: &str = "./indices/timestamps.idx";
static LOG_FILE: &str = "./bot.log";

impl Display for LogMessage {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "LogLine ({} - {} - {})",
            self.level, self.message, self.timestamp
        )
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
            .push(idx);

        for word in log.message.split_whitespace() {
            indices
                .words
                .entry(word.to_lowercase())
                .or_default()
                .push(idx);
        }

        let ts = log.timestamp.timestamp();
        indices.timestamps.entry(ts).or_default().push(idx);
    }
}

pub async fn read_file_task(
    tx: Sender<ParsedLog>,
    start_offset: u64,
    metadata: Arc<RwLock<Metadata>>,
) -> io::Result<()> {
    let mut file = File::open(LOG_FILE).await?;
    file.seek(SeekFrom::Start(start_offset)).await?;
    let mut buffer = Vec::new();
    let mut current_pos = start_offset;
    let mut chunk = vec![0u8; 1024];

    loop {
        let n = file.read(&mut chunk).await?;

        if n == 0 {
            // prevent busy waiting
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        }

        buffer.extend_from_slice(&chunk[..n]);

        while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
            let line_bytes = buffer.drain(..=pos).collect::<Vec<_>>();

            let line_len = line_bytes.len() as u64;
            let line_start_offset = current_pos;
            current_pos += line_len;

            if let Ok(line_str) = String::from_utf8(line_bytes) {
                if let Ok(log_line) = serde_json::from_str::<LogMessage>(line_str.trim()) {
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
}

async fn write_index_file_to_disk<T: Serialize>(
    indices: &HashMap<T, Vec<u64>>,
    filename: &str,
) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(filename)
        .await?;

    let bytes = postcard::to_stdvec(indices).expect("Failed to serialize indices");

    let mut encoder = Encoder::new(Vec::new(), 3)?;
    encoder.write_all(&bytes)?;
    let compressed = encoder.finish()?;

    file.write_all(&compressed).await?;
    file.flush().await?;

    Ok(())
}

async fn write_indices_to_disk(indices: Arc<RwLock<Indices>>) -> io::Result<()> {
    let indices = indices.read().await;

    write_index_file_to_disk(&indices.levels, LEVEL_INDICES_FILE).await?;
    write_index_file_to_disk(&indices.words, WORD_INDICES_FILE).await?;
    write_index_file_to_disk(&indices.timestamps, TIMESTAMP_INDICES_FILE).await?;

    Ok(())
}

async fn write_periodically(indices: Arc<RwLock<Indices>>, metadata: Arc<RwLock<Metadata>>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        if let Err(e) = write_indices_to_disk(Arc::clone(&indices)).await {
            eprintln!("Failed to write indices: {:?}", e);
        }

        let meta_guard = metadata.read().await;
        if let Err(e) = save_metadata(&*meta_guard).await {
            eprintln!("Failed to save metadata: {:?}", e);
        }
    }
}

async fn load_index_file<T: for<'de> Deserialize<'de> + Eq + Hash>(
    filename: &str,
) -> io::Result<HashMap<T, Vec<u64>>> {
    match tokio::fs::read(filename).await {
        Ok(bytes) => {
            let mut decoder = Decoder::new(&bytes[..]).expect("Decoder failed");
            let mut decompressed = Vec::new();
            
            std::io::copy(&mut decoder, &mut decompressed).expect("Decompression failed");

            let map = postcard::from_bytes(&decompressed).unwrap_or_else(|_| HashMap::new());

            Ok(map)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(e) => Err(e),
    }
}

async fn load_metadata() -> io::Result<Metadata> {
    match tokio::fs::read(METADATA_FILE).await {
        Ok(bytes) => {
            let meta = serde_json::from_slice(&bytes).unwrap_or(Metadata {
                last_offset: 0,
                last_id: 0,
            });
            Ok(meta)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Metadata {
            last_offset: 0,
            last_id: 0,
        }),
        Err(e) => Err(e),
    }
}

async fn save_metadata(meta: &Metadata) -> io::Result<()> {
    let bytes = serde_json::to_vec(meta).unwrap();
    tokio::fs::write(METADATA_FILE, bytes).await
}

async fn find_logs_by_offsets(offsets: &Vec<u64>) -> io::Result<Vec<String>> {
    let mut offsets = offsets.clone();
    offsets.sort();

    let mut res = Vec::new();
    let file = File::open(LOG_FILE).await?;
    let mut reader = tokio::io::BufReader::new(file);

    let mut current_offset = 0u64;
    let mut offset_index = 0;

    let mut line = Vec::new();

    while reader.read_until(b'\n', &mut line).await? > 0 {
        if offset_index >= offsets.len() {
            break;
        }

        let line_start = current_offset;
        let line_end = current_offset + line.len() as u64;

        while offset_index < offsets.len()
            && offsets[offset_index] >= line_start
            && offsets[offset_index] < line_end
        {
            if let Ok(line_str) = String::from_utf8(line.clone()) {
                res.push(line_str.trim().to_string());
            }
            offset_index += 1;
        }

        current_offset = line_end;
        line.clear();
    }

    Ok(res)
}

async fn query<K, Q>(indices: &HashMap<K, Vec<u64>>, needle: &Q)
where
    K: Eq + Hash + Borrow<Q>,
    Q: Eq + Hash + ?Sized,
{
    if let Some(offsets) = indices.get(needle) {
        match find_logs_by_offsets(offsets).await {
            Ok(logs) => {
                for log in logs {
                    println!("{}", log);
                }
            }
            Err(e) => {
                println!("Error retrieving logs: {}", e);
            }
        }
    } else {
        println!("No results");
    }
}
async fn cli_task(indices: Arc<RwLock<Indices>>) {
    let mut reader = BufReader::new(stdin());
    let mut line = String::new();

    println!("Log query system ready.");
    println!("Example: level=warn or word=timeout");

    loop {
        line.clear();

        if reader.read_line(&mut line).await.unwrap() == 0 {
            break;
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

#[tokio::main]
async fn main() -> io::Result<()> {
    tokio::fs::create_dir_all("./indices").await?;

    let indices = Arc::new(RwLock::new(Indices {
        levels: load_index_file("./indices/levels.idx").await?,
        words: load_index_file("./indices/words.idx").await?,
        timestamps: load_index_file("./indices/timestamps.idx").await?,
    }));

    let metadata_val = load_metadata().await?;
    NEXT_ID.store(metadata_val.last_id, Ordering::Relaxed);
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

    let reader = tokio::spawn(read_file_task(tx, start_offset, Arc::clone(&metadata)));
    let receiver = tokio::spawn(receive_log_task(rx, Arc::clone(&indices)));
    let cli = tokio::spawn(cli_task(Arc::clone(&indices)));

    let _ = tokio::join!(reader, receiver, writer, cli);

    Ok(())
}
