use crate::error::SpecError;

/// No real (or realistically future) machine has more cores than this;
/// rejecting anything past it keeps `cores_to_pcpu_percent`'s `as u32`
/// conversion from ever silently saturating to `u32::MAX` on an
/// obviously-impossible value.
const MAX_CPU_CORES: f64 = 1024.0;

pub fn parse_cpu_cores(s: &str) -> Result<f64, SpecError> {
    let cores: f64 = s
        .parse()
        .map_err(|_| SpecError::InvalidCpu(s.to_string()))?;
    let invalid = || SpecError::InvalidCpu(s.to_string());
    if !(cores > 0.0 && cores.is_finite() && cores <= MAX_CPU_CORES) {
        return Err(invalid());
    }
    // A value too tiny to round to even 1% CPU would otherwise pass this
    // function cleanly and only surface later as a silently frozen
    // (`pcpu:deny=0`) jail, with no error raised anywhere.
    if cores_to_pcpu_percent(cores) == 0 {
        return Err(invalid());
    }
    Ok(cores)
}

pub fn cores_to_pcpu_percent(cores: f64) -> u32 {
    (cores * 100.0).round() as u32
}

pub fn parse_memory_bytes(s: &str) -> Result<u64, SpecError> {
    let invalid = || SpecError::InvalidMemory(s.to_string());
    let upper = s.to_ascii_uppercase();
    let (num_part, multiplier): (&str, u64) = if let Some(n) = upper.strip_suffix('K') {
        (n, 1024)
    } else if let Some(n) = upper.strip_suffix('M') {
        (n, 1024 * 1024)
    } else if let Some(n) = upper.strip_suffix('G') {
        (n, 1024 * 1024 * 1024)
    } else {
        (upper.as_str(), 1)
    };
    let value: u64 = num_part.parse().map_err(|_| invalid())?;
    if value == 0 {
        return Err(invalid());
    }
    value.checked_mul(multiplier).ok_or_else(invalid)
}

/// A ZFS quota and a memory size are the same kind of quantity (a plain
/// byte count with an optional K/M/G suffix) — reuses `parse_memory_bytes`'s
/// grammar directly rather than inventing a new one.
pub fn parse_zfs_quota(s: &str) -> Result<u64, SpecError> {
    parse_memory_bytes(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_cpu_values() {
        assert_eq!(parse_cpu_cores("2"), Ok(2.0));
        assert_eq!(parse_cpu_cores("0.5"), Ok(0.5));
    }

    #[test]
    fn rejects_invalid_cpu_values() {
        assert_eq!(
            parse_cpu_cores("0"),
            Err(SpecError::InvalidCpu("0".to_string()))
        );
        assert_eq!(
            parse_cpu_cores("-1"),
            Err(SpecError::InvalidCpu("-1".to_string()))
        );
        assert_eq!(
            parse_cpu_cores("abc"),
            Err(SpecError::InvalidCpu("abc".to_string()))
        );
    }

    #[test]
    fn rejects_a_value_too_tiny_to_produce_any_real_cpu_share() {
        // 0.001 cores passes a bare ">0.0" check but rounds to 0% in
        // cores_to_pcpu_percent, producing a completely frozen (pcpu:deny=0)
        // jail with no error raised anywhere.
        assert_eq!(
            parse_cpu_cores("0.001"),
            Err(SpecError::InvalidCpu("0.001".to_string()))
        );
    }

    #[test]
    fn accepts_the_smallest_value_that_still_rounds_to_a_real_share() {
        assert_eq!(parse_cpu_cores("0.005"), Ok(0.005));
    }

    #[test]
    fn rejects_an_absurdly_large_value_instead_of_silently_saturating() {
        // 1e9 cores is finite, so it passed the old check, but
        // cores_to_pcpu_percent's `as u32` conversion silently saturates
        // to u32::MAX rather than erroring on an obviously-impossible
        // value for any real (or realistically future) machine.
        assert_eq!(
            parse_cpu_cores("1e9"),
            Err(SpecError::InvalidCpu("1e9".to_string()))
        );
    }

    #[test]
    fn accepts_a_generously_high_but_plausible_core_count() {
        assert_eq!(parse_cpu_cores("1024"), Ok(1024.0));
    }

    #[test]
    fn converts_cores_to_pcpu_percent() {
        assert_eq!(cores_to_pcpu_percent(2.0), 200);
        assert_eq!(cores_to_pcpu_percent(0.5), 50);
    }

    #[test]
    fn parses_valid_memory_values() {
        assert_eq!(parse_memory_bytes("512M"), Ok(512 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("1G"), Ok(1024 * 1024 * 1024));
        assert_eq!(parse_memory_bytes("2048K"), Ok(2048 * 1024));
        assert_eq!(parse_memory_bytes("100"), Ok(100));
    }

    #[test]
    fn rejects_invalid_memory_values() {
        assert!(parse_memory_bytes("0M").is_err());
        assert!(parse_memory_bytes("").is_err());
        assert!(parse_memory_bytes("abc").is_err());
        assert!(parse_memory_bytes("-5M").is_err());
        assert!(parse_memory_bytes("999999999999G").is_err());
    }

    #[test]
    fn parse_zfs_quota_accepts_the_same_grammar_as_memory() {
        assert_eq!(parse_zfs_quota("1G"), Ok(1024 * 1024 * 1024));
        assert_eq!(parse_zfs_quota("512M"), Ok(512 * 1024 * 1024));
    }

    #[test]
    fn parse_zfs_quota_rejects_the_same_malformed_input_as_memory() {
        assert!(parse_zfs_quota("0G").is_err());
        assert!(parse_zfs_quota("abc").is_err());
    }
}
