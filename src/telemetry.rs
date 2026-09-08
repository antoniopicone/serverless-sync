//! Telemetria verso il logger.
//!
//! Fire-and-forget, FUORI dal percorso di sync: un errore qui non deve mai
//! influenzare la convergenza. Se il logger e' spento i nodi non se ne
//! accorgono — ed e' una proprieta' che lo scenario di test verifica
//! esplicitamente, perche' un osservatore che diventa una dipendenza e'
//! esattamente il punto centrale che questa architettura nega.

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

    /// Non attende la risposta e ignora ogni errore, per costruzione.
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
