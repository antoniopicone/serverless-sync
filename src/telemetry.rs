//! Telemetry toward the logger.
//!
//! Fire-and-forget, OUTSIDE the sync path: an error here must never
//! affect convergence. If the logger is off, nodes don't notice — a
//! property the test scenario checks explicitly, because an observer that
//! becomes a dependency is exactly the central point this architecture
//! denies.

use serde_json::json;

#[derive(Clone)]
pub struct Telemetry {
    url: Option<String>,
    device: String,
    http: reqwest::Client,
}

impl Telemetry {
    pub fn new(url: Option<String>, device: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(800))
            .build()
            .unwrap_or_default();
        Self { url, device, http }
    }

    /// Doesn't wait for the response and ignores every error, by design.
    pub fn emit(&self, kind: &str, data: serde_json::Value) {
        let Some(url) = self.url.clone() else { return };
        let body = json!({
            "device": self.device,
            "kind": kind,
            "data": data,
        });
        let http = self.http.clone();
        tokio::spawn(async move {
            let _ = http.post(&url).json(&body).send().await;
        });
    }
}
