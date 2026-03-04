use crate::error::LogQueryError;
use crate::metadata::{Metadata, save_metadata};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::{collections::BTreeSet, hash::Hash, io::Write, sync::Arc, time::Duration};
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::RwLock, time};
use zstd::{Decoder, Encoder};

#[derive(Debug, Default)]
pub struct Indices {
    pub levels: BTreeMap<String, BTreeSet<u64>>,
    pub words: BTreeMap<String, BTreeSet<u64>>,
    pub rev_words: BTreeMap<String, BTreeSet<u64>>,
    pub timestamps: BTreeMap<i64, BTreeSet<u64>>,
}

pub async fn write_index_file_to_disk<T: Serialize + Eq + Hash>(
    indices: &BTreeMap<T, BTreeSet<u64>>,
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

pub async fn write_indices_to_disk(
    indices: Arc<RwLock<Indices>>,
    base_dir: &str,
) -> Result<(), LogQueryError> {
    let indices = indices.read().await;

    write_index_file_to_disk(&indices.levels, &format!("{}/levels.idx", base_dir)).await?;
    write_index_file_to_disk(&indices.words, &format!("{}/words.idx", base_dir)).await?;
    write_index_file_to_disk(&indices.rev_words, &format!("{}/rev_words.idx", base_dir)).await?;
    write_index_file_to_disk(&indices.timestamps, &format!("{}/timestamps.idx", base_dir)).await?;

    Ok(())
}

pub async fn load_index_file<T: for<'de> DeserializeOwned + Eq + Hash + Ord>(
    filename: &str,
) -> Result<BTreeMap<T, BTreeSet<u64>>, LogQueryError> {
    match tokio::fs::read(filename).await {
        Ok(bytes) => {
            let mut decoder = Decoder::new(&bytes[..])?;
            let mut decompressed = Vec::new();

            std::io::copy(&mut decoder, &mut decompressed)?;
            let map = postcard::from_bytes(&decompressed).unwrap_or_default();

            Ok(map)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(e) => Err(e.into()),
    }
}

pub async fn write_periodically(
    indices: Arc<RwLock<Indices>>,
    metadata: Arc<RwLock<Metadata>>,
    base_dir: String,
) {
    loop {
        time::sleep(Duration::from_secs(5)).await;

        if let Err(e) = write_indices_to_disk(Arc::clone(&indices), &base_dir).await {
            eprintln!("Failed to write indices: {:?}", e);
        }

        let meta_guard = metadata.read().await;
        if let Err(e) = save_metadata(&meta_guard, &format!("{}/metadata.json", base_dir)).await {
            eprintln!("Failed to save metadata: {:?}", e);
        }
    }
}
