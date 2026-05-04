//! Token-bucket rate limiter used by the sender to pace UDP transmits.
//!
//! The original code used `(chunk_size_bits) / (max_speed_kbps * 1000)` to
//! compute a fixed sleep period and `thread::sleep`d that long after every
//! chunk. That was correct on average but had two bugs:
//!
//! * If the loop happened to take a few ms longer than the sleep period
//!   (because of a long disk read or a context switch), the rate dropped –
//!   it could not "catch up" by sending a burst.
//! * The `if file_sending_period > 44.0 { … = 44.0 }` clamp silently capped
//!   the slowest possible rate, which is exactly the wrong direction (you
//!   want a *minimum* sleep to avoid blowing out the radio, not a maximum).
//!
//! A token bucket fixes both: tokens accrue at the configured rate, the
//! sender takes one per chunk, and brief stalls are smoothed over by the
//! bucket.

use std::time::{Duration, Instant};

/// Simple token-bucket limiter measured in bytes/sec.
#[derive(Debug)]
pub struct RateLimiter {
    bytes_per_sec: f64,
    burst_bytes: f64,
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    /// Construct a limiter for `kbps` kilobits per second, allowing a burst
    /// equal to one second's worth of traffic.
    pub fn new(kbps: u64) -> Self {
        let bytes_per_sec = (kbps as f64) * 1000.0 / 8.0;
        Self {
            bytes_per_sec,
            burst_bytes: bytes_per_sec.max(1.0),
            tokens: bytes_per_sec.max(1.0),
            last: Instant::now(),
        }
    }

    /// Update the rate without resetting the bucket.
    pub fn set_kbps(&mut self, kbps: u64) {
        self.bytes_per_sec = (kbps as f64) * 1000.0 / 8.0;
        self.burst_bytes = self.bytes_per_sec.max(1.0);
    }

    /// Block until `n` bytes worth of tokens are available, then consume them.
    pub fn acquire(&mut self, n: usize) {
        let n = n as f64;
        loop {
            self.refill();
            if self.tokens >= n {
                self.tokens -= n;
                return;
            }
            let needed = n - self.tokens;
            let secs = needed / self.bytes_per_sec.max(1.0);
            std::thread::sleep(Duration::from_secs_f64(secs));
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.bytes_per_sec).min(self.burst_bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pacing_holds_average() {
        // 80 kbit/s = 10_000 bytes/s. Sending 5 × 2_000-byte chunks should
        // take ~1 second once the burst is drained.
        let mut rl = RateLimiter::new(80);
        // Drain the initial burst.
        rl.acquire(10_000);
        let start = Instant::now();
        for _ in 0..5 {
            rl.acquire(2_000);
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(900),
            "expected ~1s, got {:?}",
            elapsed
        );
    }
}
