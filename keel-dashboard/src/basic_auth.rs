use base64::{engine::general_purpose::STANDARD, Engine as _};

/// Checks a raw `Authorization` header value against the configured
/// username/password. Returns `false` uniformly for a missing header, a
/// non-`Basic` scheme, malformed base64/UTF-8, or a mismatched
/// user/password - the caller responds `401` in every case without
/// distinguishing why, so as not to leak which part of the credential was
/// wrong.
pub fn check(header: Option<&str>, expected_user: &str, expected_password: &str) -> bool {
    let Some(header) = header else { return false };
    let Some(encoded) = header.strip_prefix("Basic ") else { return false };
    let Ok(decoded_bytes) = STANDARD.decode(encoded) else { return false };
    let Ok(decoded) = String::from_utf8(decoded_bytes) else { return false };
    let Some((user, password)) = decoded.split_once(':') else { return false };
    user == expected_user && password == expected_password
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    fn header_for(user: &str, password: &str) -> String {
        format!("Basic {}", STANDARD.encode(format!("{user}:{password}")))
    }

    #[test]
    fn correct_credentials_pass() {
        assert!(check(Some(&header_for("admin", "hunter2")), "admin", "hunter2"));
    }

    #[test]
    fn wrong_password_fails() {
        assert!(!check(Some(&header_for("admin", "wrong")), "admin", "hunter2"));
    }

    #[test]
    fn wrong_user_fails() {
        assert!(!check(Some(&header_for("someone-else", "hunter2")), "admin", "hunter2"));
    }

    #[test]
    fn missing_header_fails() {
        assert!(!check(None, "admin", "hunter2"));
    }

    #[test]
    fn non_basic_scheme_fails() {
        assert!(!check(Some("Bearer sometoken"), "admin", "hunter2"));
    }

    #[test]
    fn malformed_base64_fails() {
        assert!(!check(Some("Basic not-valid-base64!!"), "admin", "hunter2"));
    }

    #[test]
    fn missing_colon_separator_fails() {
        assert!(!check(Some(&format!("Basic {}", STANDARD.encode("no-colon-here"))), "admin", "hunter2"));
    }
}
