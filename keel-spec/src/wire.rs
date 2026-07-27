use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
}

pub fn error_response(status: u16, message: String) -> (u16, Vec<u8>) {
    let body = serde_yaml::to_string(&ErrorBody { error: message })
        .expect("ErrorBody serialization should not fail");
    (status, body.into_bytes())
}

pub fn yaml_response<T: Serialize>(status: u16, value: &T) -> (u16, Vec<u8>) {
    let body = serde_yaml::to_string(value).expect("wire type serialization should not fail");
    (status, body.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_response_carries_the_status_and_yaml_encodes_the_message() {
        let (status, body) = error_response(404, "jail 'web-1' not found".to_string());
        assert_eq!(status, 404);
        let parsed: ErrorBody = serde_yaml::from_str(std::str::from_utf8(&body).unwrap()).unwrap();
        assert_eq!(parsed.error, "jail 'web-1' not found");
    }

    #[test]
    fn yaml_response_carries_the_status_and_yaml_encodes_the_value() {
        let (status, body) = yaml_response(200, &ErrorBody { error: "ok".to_string() });
        assert_eq!(status, 200);
        let parsed: ErrorBody = serde_yaml::from_str(std::str::from_utf8(&body).unwrap()).unwrap();
        assert_eq!(parsed.error, "ok");
    }
}
