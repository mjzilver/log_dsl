use std::{fmt, io};

#[derive(Debug)]
pub enum LogQueryError {
    Io(io::Error),
    Postcard(postcard::Error),
    Utf8(std::string::FromUtf8Error),
    SerdeJson(serde_json::Error),
}

impl fmt::Display for LogQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogQueryError::Io(e) => write!(f, "IO error: {}", e),
            LogQueryError::Postcard(e) => write!(f, "postcard error: {}", e),
            LogQueryError::Utf8(e) => write!(f, "utf8 parse error: {}", e),
            LogQueryError::SerdeJson(e) => write!(f, "serde_json error: {}", e),
        }
    }
}

impl std::error::Error for LogQueryError {}

impl From<io::Error> for LogQueryError {
    fn from(e: io::Error) -> Self {
        LogQueryError::Io(e)
    }
}

impl From<postcard::Error> for LogQueryError {
    fn from(e: postcard::Error) -> Self {
        LogQueryError::Postcard(e)
    }
}

impl From<std::string::FromUtf8Error> for LogQueryError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        LogQueryError::Utf8(e)
    }
}

impl From<serde_json::Error> for LogQueryError {
    fn from(e: serde_json::Error) -> Self {
        LogQueryError::SerdeJson(e)
    }
}
