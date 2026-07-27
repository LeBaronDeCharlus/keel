use std::process::Command;

pub fn detect() -> Result<(f64, u64), String> {
    let cpu = run_sysctl("hw.ncpu")?
        .parse::<f64>()
        .map_err(|e| format!("invalid hw.ncpu value: {e}"))?;
    let memory = run_sysctl("hw.physmem")?
        .parse::<u64>()
        .map_err(|e| format!("invalid hw.physmem value: {e}"))?;
    Ok((cpu, memory))
}

fn run_sysctl(name: &str) -> Result<String, String> {
    let output = Command::new("sysctl")
        .arg("-n")
        .arg(name)
        .output()
        .map_err(|e| format!("failed to run sysctl -n {name}: {e}"))?;
    if !output.status.success() {
        return Err(format!("sysctl -n {name} exited with {}", output.status));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

// `detect()` shells out to the real `sysctl` binary with BSD OIDs
// (`hw.ncpu`, `hw.physmem`). Those exist on FreeBSD (and macOS, which
// inherits BSD sysctl naming) but not on Linux, where `sysctl` has no `hw`
// namespace at all. Gated the same way `tests/freebsd_*.rs` already gates
// real-driver coverage, so `cargo test --workspace` on the Linux CI runner
// skips this module rather than failing deterministically.
#[cfg(all(test, target_os = "freebsd"))]
mod tests {
    use super::*;

    #[test]
    fn detect_returns_positive_cpu_and_memory() {
        let (cpu, memory) =
            detect().expect("sysctl -n hw.ncpu / hw.physmem should succeed on any BSD-derived OS");
        assert!(cpu > 0.0, "expected a positive CPU count, got {cpu}");
        assert!(memory > 0, "expected a positive memory size, got {memory}");
    }
}
