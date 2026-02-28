use crate::error::LogQueryError;
use crate::metadata::{Metadata, save_metadata};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::{
    collections::{BTreeSet, HashMap},
    hash::Hash,
    io::Write,
    sync::Arc,
    time::Duration,
};
use tokio::time;
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::RwLock};
use zstd::{Decoder, Encoder};

pub const LEVEL_INDICES_FILE: &str = "./indices/levels.idx";
pub const WORD_INDICES_FILE: &str = "./indices/words.idx";
pub const TIMESTAMP_INDICES_FILE: &str = "./indices/timestamps.idx";

#[derive(Debug, Default)]
pub struct Indices {
    pub levels: HashMap<String, BTreeSet<u64>>,
    pub words: HashMap<String, BTreeSet<u64>>,
    pub timestamps: HashMap<i64, BTreeSet<u64>>,
}

pub async fn write_index_file_to_disk<T: Serialize + Eq + Hash>(
    indices: &HashMap<T, BTreeSet<u64>>,
    filename: &str,
) -> Result<(), LogQueryError> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(filename)
        .await?;

    let bytes = postcard::to_stdvec(indices)?;

    let mut encoder = Encoder::new(Vec::new(), 3)?;
    encoder.write_all(&bytes)?;
    let compressed = encoder.finish()?;

    file.write_all(&compressed).await?;
    file.flush().await?;

    Ok(())
}

pub async fn write_indices_to_disk(indices: Arc<RwLock<Indices>>) -> Result<(), LogQueryError> {
    let indices = indices.read().await;

    write_index_file_to_disk(&indices.levels, LEVEL_INDICES_FILE).await?;
    write_index_file_to_disk(&indices.words, WORD_INDICES_FILE).await?;
    write_index_file_to_disk(&indices.timestamps, TIMESTAMP_INDICES_FILE).await?;

    Ok(())
}

pub async fn load_index_file<T: for<'de> DeserializeOwned + Eq + Hash>(
    filename: &str,
) -> Result<HashMap<T, BTreeSet<u64>>, LogQueryError> {
    match tokio::fs::read(filename).await {
        Ok(bytes) => {
            let mut decoder = Decoder::new(&bytes[..])?;
            let mut decompressed = Vec::new();

            std::io::copy(&mut decoder, &mut decompressed)?;
            let map = match postcard::from_bytes(&decompressed) {
                Ok(m) => m,
                Err(_) => HashMap::new(),
            };

            Ok(map)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(e) => Err(e.into()),
    }
}

pub async fn write_periodically(indices: Arc<RwLock<Indices>>, metadata: Arc<RwLock<Metadata>>) {
    loop {
        time::sleep(Duration::from_secs(5)).await;

        if let Err(e) = write_indices_to_disk(Arc::clone(&indices)).await {
            eprintln!("Failed to write indices: {:?}", e);
        }

        let meta_guard = metadata.read().await;
        if let Err(e) = save_metadata(&meta_guard).await {
            eprintln!("Failed to save metadata: {:?}", e);
        }
    }
}
