use thiserror::Error;

#[derive(Debug, Error)]
pub enum NetError {
    #[error("failed to spawn `{0}`: {1}")]
    Spawn(String, std::io::Error),
    #[error("`{0}` failed with exit status {1}: {2}")]
    CommandFailed(String, std::process::ExitStatus, String),
    #[error("bridge '{0}' not found")]
    NotFound(String),
    #[error("invalid network address '{0}'")]
    InvalidAddress(String),
}
