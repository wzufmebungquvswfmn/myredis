use thiserror::Error;

#[derive(Debug, Error)]
pub enum MyRedisError {
    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("command error: {0}")]
    Command(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("incomplete frame")]
    Incomplete,
}

pub type Result<T> = std::result::Result<T, MyRedisError>;
