use crate::error::LogQueryError;
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Metadata {
    pub last_offset: u64,
    pub filename: String,
    pub file_id: String,
}

pub async fn load_metadata(
    metadata_path: &str,
    filename: &str,
    file_id: &str,
) -> Result<Metadata, LogQueryError> {
    match tokio::fs::read(metadata_path).await {
        Ok(bytes) => {
            let meta: Result<Metadata, _> = serde_json::from_slice(&bytes);
            match meta {
                Ok(m) => Ok(m),
                Err(_) => Ok(Metadata {
                    last_offset: 0,
                    filename: filename.to_string(),
                    file_id: file_id.to_string(),
                }),
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Metadata {
            last_offset: 0,
            filename: filename.to_string(),
            file_id: file_id.to_string(),
        }),
        Err(e) => Err(e.into()),
    }
}

pub async fn save_metadata(meta: &Metadata, metadata_path: &str) -> Result<(), LogQueryError> {
    let bytes = serde_json::to_vec(meta)?;
    tokio::fs::write(metadata_path, bytes).await?;
    Ok(())
}
