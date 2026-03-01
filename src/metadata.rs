use crate::error::LogQueryError;
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Metadata {
    pub last_offset: u64,
}

static METADATA_FILE: &str = "./indices/metadata.json";

pub async fn load_metadata() -> Result<Metadata, LogQueryError> {
    match tokio::fs::read(METADATA_FILE).await {
        Ok(bytes) => {
            let meta = match serde_json::from_slice(&bytes) {
                Ok(m) => m,
                Err(_) => Metadata { last_offset: 0 },
            };
            Ok(meta)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Metadata { last_offset: 0 }),
        Err(e) => Err(e.into()),
    }
}

pub async fn save_metadata(meta: &Metadata) -> Result<(), LogQueryError> {
    let bytes = serde_json::to_vec(meta)?;
    tokio::fs::write(METADATA_FILE, bytes).await?;
    Ok(())
}
