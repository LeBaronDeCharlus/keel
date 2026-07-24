use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum SpecError {
    #[error("failed to parse YAML: {0}")]
    Yaml(String),
    #[error("invalid jail name '{0}': must be 1-63 lowercase alphanumeric characters or hyphens, starting and ending with an alphanumeric character")]
    InvalidName(String),
    #[error("invalid network address '{0}': {1}")]
    InvalidAddress(String, String),
    #[error("invalid cpu value '{0}': must be a positive number of cores")]
    InvalidCpu(String),
    #[error("invalid memory value '{0}': expected a number optionally followed by K, M, or G")]
    InvalidMemory(String),
    #[error("invalid port {0}: must be non-zero")]
    InvalidPort(u16),
    #[error("duplicate volume name '{0}' in spec.volumes")]
    DuplicateVolumeName(String),
    #[error("field '{0}' cannot be changed after the jail is created; delete and re-apply instead")]
    ImmutableField(&'static str),
    #[error("invalid host '{0}': must be a syntactically well-formed DNS name")]
    InvalidHost(String),
    #[error("invalid email '{0}': must be a syntactically well-formed email address")]
    InvalidEmail(String),
    #[error("invalid image '{0}': must start with 'base/' followed by lowercase alphanumeric segments separated by '-', '.', or '/'")]
    InvalidImage(String),
    #[error("invalid mountPath '{0}': must be an absolute path with no '..' segments")]
    InvalidMountPath(String),
    #[error("invalid bridge '{0}': must be 'keel' followed by digits, e.g. 'keel0'")]
    InvalidBridge(String),
}
