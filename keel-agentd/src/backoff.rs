use std::time::{Duration, Instant};

use crate::wire::BackoffStatus;

const INITIAL_DELAY: Duration = Duration::from_secs(1);
const MAX_DELAY: Duration = Duration::from_secs(300);
const RESET_UPTIME_THRESHOLD: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct BackoffState {
    current_delay: Duration,
    next_retry_at: Option<Instant>,
    last_started_at: Option<Instant>,
}

impl Default for BackoffState {
    fn default() -> Self {
        Self {
            current_delay: INITIAL_DELAY,
            next_retry_at: None,
            last_started_at: None,
        }
    }
}

impl BackoffState {
    pub fn new() -> Self {
        Self::default()
    }

    /// True if there's no active cooldown, or the cooldown has passed.
    pub fn can_retry(&self, now: Instant) -> bool {
        match self.next_retry_at {
            Some(t) => now >= t,
            None => true,
        }
    }

    /// Call this every time an action (provisioning attempt or restart) is
    /// taken for this jail, regardless of whether that action succeeded —
    /// a successful `start_command` carries no information about whether
    /// the process will keep running, so the cooldown must still be armed.
    pub fn record_attempt(&mut self, now: Instant) {
        self.last_started_at = Some(now);
        self.next_retry_at = Some(now + self.current_delay);
        self.current_delay = (self.current_delay * 2).min(MAX_DELAY);
    }

    /// Call this whenever the jail is actually observed running (the
    /// "exists && running" happy path in `reconcile_one`, on every tick,
    /// not just once) — proactively resets the escalated delay back to
    /// `INITIAL_DELAY` once the jail has been confirmed running for at
    /// least `RESET_UPTIME_THRESHOLD` since its last (re)start attempt.
    ///
    /// This is the only place genuine uptime is observable. A reconciler
    /// never retries before its own armed cooldown, so the gap between
    /// successive `record_attempt` calls is mechanically >= the delay just
    /// armed — using that gap as a stable-uptime signal (as this used to)
    /// meant escalation could never survive much past
    /// `RESET_UPTIME_THRESHOLD` of delay: once armed delay reached it, every
    /// subsequent attempt looked like "plenty of uptime" and reset,
    /// oscillating short delays forever instead of ever reaching
    /// `MAX_DELAY` for a jail that never actually ran successfully.
    pub fn note_running(&mut self, now: Instant) {
        if let Some(last) = self.last_started_at {
            if now.saturating_duration_since(last) >= RESET_UPTIME_THRESHOLD {
                self.current_delay = INITIAL_DELAY;
            }
        }
    }

    /// Read-only snapshot for the HTTP API's `get`/`list` — reports the
    /// delay in whole seconds relative to `now`, not an absolute timestamp
    /// (an `Instant` has no wall-clock meaning to report as one).
    pub fn status(&self, now: Instant) -> BackoffStatus {
        match self.next_retry_at {
            Some(next) => BackoffStatus {
                retry_in_secs: Some(next.saturating_duration_since(now).as_secs()),
                current_delay_secs: Some(self.current_delay.as_secs()),
            },
            None => BackoffStatus::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_state_can_retry_immediately() {
        let state = BackoffState::new();
        assert!(state.can_retry(Instant::now()));
    }

    #[test]
    fn cannot_retry_until_delay_passes() {
        let mut state = BackoffState::new();
        let t0 = Instant::now();
        state.record_attempt(t0);
        assert!(!state.can_retry(t0));
        assert!(!state.can_retry(t0 + Duration::from_millis(500)));
        assert!(state.can_retry(t0 + Duration::from_secs(1)));
    }

    #[test]
    fn backoff_escalates_on_rapid_repeated_failures() {
        let mut state = BackoffState::new();
        let t0 = Instant::now();
        state.record_attempt(t0); // next_retry_at = t0 + 1s, current_delay becomes 2s
        let t1 = t0 + Duration::from_secs(1);
        state.record_attempt(t1); // next_retry_at = t1 + 2s, current_delay becomes 4s
        assert!(!state.can_retry(t1 + Duration::from_secs(1)));
        assert!(state.can_retry(t1 + Duration::from_secs(2)));
    }

    #[test]
    fn backoff_resets_after_confirmed_running_uptime() {
        let mut state = BackoffState::new();
        let t0 = Instant::now();
        state.record_attempt(t0); // current_delay becomes 2s after this

        // The jail is actually observed running (reconcile_one's "exists &&
        // running" happy path) continuously for 60+ seconds since the
        // attempt above -- the only genuine signal of real uptime.
        let t1 = t0 + Duration::from_secs(61);
        state.note_running(t1);

        // It then crashes again: the escalated delay must have been reset
        // by the confirmed uptime, not still be climbing from the earlier
        // failure.
        state.record_attempt(t1); // should reset to 1s (not escalate to 4s) before doubling to 2s
        assert!(!state.can_retry(t1 + Duration::from_millis(500)));
        assert!(state.can_retry(t1 + Duration::from_secs(1)));
    }

    #[test]
    fn note_running_before_sixty_seconds_of_uptime_does_not_reset_the_escalation() {
        let mut state = BackoffState::new();
        let t0 = Instant::now();
        state.record_attempt(t0); // current_delay becomes 2s after this
        state.record_attempt(t0 + Duration::from_secs(1)); // current_delay becomes 4s

        // Only 30s of confirmed uptime -- must not reset yet.
        state.note_running(t0 + Duration::from_secs(31));
        state.record_attempt(t0 + Duration::from_secs(31));
        assert!(!state.can_retry(t0 + Duration::from_secs(31) + Duration::from_secs(3)));
        assert!(state.can_retry(t0 + Duration::from_secs(31) + Duration::from_secs(4)));
    }

    #[test]
    fn backoff_escalates_to_the_cap_under_a_realistic_retry_cadence_without_spuriously_resetting() {
        // Reproduces a real bug: a reconciler never retries before its own
        // armed cooldown, so the gap between successive `record_attempt`
        // calls is mechanically >= the delay just armed. If `record_attempt`
        // itself treated "long gap since the last attempt" as proof of
        // stable uptime (rather than requiring an explicit `note_running`
        // observation), escalation would never survive past ~60s of delay --
        // oscillating between short delays forever instead of ever reaching
        // the 5-minute cap, for a jail that never actually ran successfully
        // even once.
        let mut state = BackoffState::new();
        let mut now = Instant::now();
        for _ in 0..12 {
            let delay = state.status(now).current_delay_secs.unwrap_or(1);
            now += Duration::from_secs(delay);
            state.record_attempt(now);
        }
        assert!(!state.can_retry(now + Duration::from_secs(299)));
        assert!(state.can_retry(now + Duration::from_secs(300)));
    }

    #[test]
    fn backoff_caps_at_five_minutes() {
        let mut state = BackoffState::new();
        let mut now = Instant::now();
        for _ in 0..20 {
            now += Duration::from_secs(1); // always retrying immediately, never resetting
            state.record_attempt(now);
        }
        // After enough rapid escalations, the delay should be capped at 300s.
        assert!(!state.can_retry(now + Duration::from_secs(299)));
        assert!(state.can_retry(now + Duration::from_secs(300)));
    }

    #[test]
    fn status_reports_no_cooldown_for_a_fresh_state() {
        let state = BackoffState::new();
        let status = state.status(Instant::now());
        assert_eq!(status.retry_in_secs, None);
        assert_eq!(status.current_delay_secs, None);
    }

    #[test]
    fn status_reports_retry_in_secs_and_current_delay_after_an_attempt() {
        let mut state = BackoffState::new();
        let t0 = Instant::now();
        state.record_attempt(t0); // next_retry_at = t0 + 1s, current_delay becomes 2s

        let status = state.status(t0);
        assert_eq!(status.retry_in_secs, Some(1));
        assert_eq!(status.current_delay_secs, Some(2));

        let later = state.status(t0 + Duration::from_millis(500));
        assert_eq!(
            later.retry_in_secs,
            Some(0),
            "500ms remaining rounds down to 0 whole seconds"
        );
    }
}
