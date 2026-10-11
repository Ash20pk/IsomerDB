//! Client-side token buckets per model: requests per minute and tokens per minute.

use std::time::{Duration, Instant};

use tokio::sync::Mutex;

pub struct RateLimiter {
    state: Mutex<State>,
    requests_per_sec: f64,
    tokens_per_sec: f64,
    request_capacity: f64,
    token_capacity: f64,
}

struct State {
    requests: f64,
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    pub fn new(requests_per_min: f64, tokens_per_min: f64) -> Self {
        let request_capacity = requests_per_min.max(1.0);
        let token_capacity = tokens_per_min.max(1.0);
        RateLimiter {
            state: Mutex::new(State { requests: request_capacity, tokens: token_capacity, last: Instant::now() }),
            requests_per_sec: request_capacity / 60.0,
            tokens_per_sec: token_capacity / 60.0,
            request_capacity,
            token_capacity,
        }
    }

    /// Wait until one request carrying `tokens` tokens fits in both buckets, then take it.
    /// A batch larger than the whole token bucket is let through once the bucket is full, so an
    /// oversized batch can never wait forever.
    pub async fn acquire(&self, tokens: f64) {
        let tokens = tokens.min(self.token_capacity);
        loop {
            let wait = {
                let mut s = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(s.last).as_secs_f64();
                s.last = now;
                s.requests = (s.requests + elapsed * self.requests_per_sec).min(self.request_capacity);
                s.tokens = (s.tokens + elapsed * self.tokens_per_sec).min(self.token_capacity);
                if s.requests >= 1.0 && s.tokens >= tokens {
                    s.requests -= 1.0;
                    s.tokens -= tokens;
                    return;
                }
                let need_requests = ((1.0 - s.requests) / self.requests_per_sec).max(0.0);
                let need_tokens = ((tokens - s.tokens) / self.tokens_per_sec).max(0.0);
                need_requests.max(need_tokens)
            };
            tokio::time::sleep(Duration::from_secs_f64(wait.max(0.005))).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn waits_when_the_request_bucket_is_empty() {
        let limiter = RateLimiter::new(60.0, 1_000_000.0); // 1 request per second, burst 60
        for _ in 0..60 {
            limiter.acquire(1.0).await;
        }
        let start = Instant::now();
        limiter.acquire(1.0).await;
        assert!(start.elapsed() >= Duration::from_millis(900));
    }
}
