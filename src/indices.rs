use crate::error::LogQueryError;
use crate::metadata::{Metadata, save_metadata};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::BTreeSet, hash::Hash, io::Write, sync::Arc, time::Duration};
use tokio::fs;
use tokio::sync::Notify;
use tokio::{fs::OpenOptions, io::AsyncWriteExt, sync::RwLock, time};
use zstd::{Decoder, Encoder};

#[derive(Debug, Default)]
pub struct Indices {
    pub levels: BTreeMap<String, BTreeSet<u64>>,
    pub words: BTreeMap<String, BTreeSet<u64>>,
    pub rev_words: BTreeMap<String, BTreeSet<u64>>,
    pub timestamps: BTreeMap<i64, BTreeSet<u64>>,
    pub dirty: AtomicBool,
    pub notify: Arc<Notify>,
}

pub async fn write_index_file_to_disk<T: Serialize + Eq + Hash>(
    indices: &BTreeMap<T, BTreeSet<u64>>,
    filename: &str,
) -> Result<(), LogQueryError> {
    let tmp_filename = format!("{}.tmp", filename);

    let mut tmp_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_filename)
        .await?;

    let bytes = postcard::to_stdvec(indices)?;

    let mut encoder = Encoder::new(Vec::new(), 3)?;
    encoder.write_all(&bytes)?;
    let compressed = encoder.finish()?;

    tmp_file.write_all(&compressed).await?;
    tmp_file.sync_all().await?;
    drop(tmp_file);

    fs::rename(&tmp_filename, filename).await?;

    if let Some(parent) = Path::new(filename).parent() {
        let dir = OpenOptions::new().read(true).open(parent).await?;
        dir.sync_all().await?;
    }

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

    indices.dirty.store(false, Ordering::Relaxed);

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
    let periodic_flush = Duration::from_secs(30);

    loop {
        let notify = {
            let guard = indices.read().await;
            guard.notify.clone()
        };

        tokio::select! {
            _ = notify.notified() => {},
            _ = time::sleep(periodic_flush) => {},
        }

        loop {
            time::sleep(Duration::from_secs(5)).await;

            let should_write = indices.read().await.dirty.swap(false, Ordering::Relaxed);
            if !should_write {
                break;
            }

            if let Err(e) = write_indices_to_disk(Arc::clone(&indices), &base_dir).await {
                eprintln!("Failed to write indices: {:?}", e);
            }

            let meta_guard = metadata.read().await;
            if let Err(e) = save_metadata(&meta_guard, &format!("{}/metadata.json", base_dir)).await
            {
                eprintln!("Failed to save metadata: {:?}", e);
            }
        }
    }
}
