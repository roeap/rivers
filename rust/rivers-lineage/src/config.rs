//! Where and how the emitter delivers OpenLineage events.

use std::time::Duration;

use url::Url;

/// Endpoint path of the OpenLineage HTTP API, relative to [`LineageConfig::url`].
pub const DEFAULT_ENDPOINT: &str = "/api/v1/lineage";

/// Settings for the lineage emitter.
///
/// Read from `RIVERS_LINEAGE_*`, falling back to the `OPENLINEAGE_*` variables
/// the rest of the OpenLineage ecosystem reads, so one environment configures
/// rivers and the engines it runs.
#[derive(Debug, Clone)]
pub struct LineageConfig {
    /// Base URL of the lineage backend, e.g. `http://headwaters:8091`.
    pub url: Url,
    /// Path of the single-event endpoint; the batch endpoint is this plus `/batch`.
    pub endpoint: String,
    /// Bearer token sent with every request.
    pub api_key: Option<String>,
    /// Job namespace. `None` derives `rivers://<code location>`.
    pub namespace: Option<String>,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Pause between polls of the event log once it is drained.
    pub poll_interval: Duration,
    /// How long a row must have been in the log before it is read. Covers the
    /// gap between a writer reading the server clock and its commit landing.
    pub settle: Duration,
    /// Most events read per poll.
    pub batch_size: usize,
}

impl LineageConfig {
    /// Config with defaults for everything but the backend URL.
    pub fn new(url: Url) -> Self {
        Self {
            url,
            endpoint: DEFAULT_ENDPOINT.to_string(),
            api_key: None,
            namespace: None,
            timeout: Duration::from_secs(30),
            poll_interval: Duration::from_secs(1),
            settle: Duration::from_secs(2),
            batch_size: 500,
        }
    }

    /// Read the config from the process environment. `None` when no URL is
    /// set, or `RIVERS_LINEAGE_ENABLED` is `false`/`0`: lineage is off.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// [`Self::from_env`] over an injected lookup, so tests don't touch the
    /// process environment.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Option<Self>> {
        let get = |rivers: &str, openlineage: Option<&str>| {
            lookup(rivers)
                .or_else(|| openlineage.and_then(&lookup))
                .filter(|v| !v.is_empty())
        };
        if let Some(enabled) = get("RIVERS_LINEAGE_ENABLED", None)
            && matches!(enabled.to_ascii_lowercase().as_str(), "false" | "0" | "no")
        {
            return Ok(None);
        }
        let Some(url) = get("RIVERS_LINEAGE_URL", Some("OPENLINEAGE_URL")) else {
            return Ok(None);
        };
        let url = Url::parse(&url)
            .map_err(|e| anyhow::anyhow!("invalid RIVERS_LINEAGE_URL {url:?}: {e}"))?;
        let mut cfg = Self::new(url);
        if let Some(endpoint) = get("RIVERS_LINEAGE_ENDPOINT", Some("OPENLINEAGE_ENDPOINT")) {
            cfg.endpoint = endpoint;
        }
        cfg.api_key = get("RIVERS_LINEAGE_API_KEY", Some("OPENLINEAGE_API_KEY"));
        cfg.namespace = get("RIVERS_LINEAGE_NAMESPACE", Some("OPENLINEAGE_NAMESPACE"));
        if let Some(ms) = get("RIVERS_LINEAGE_TIMEOUT_MS", Some("OPENLINEAGE_TIMEOUT_MS")) {
            cfg.timeout = Duration::from_millis(parse_num("RIVERS_LINEAGE_TIMEOUT_MS", &ms)?);
        }
        if let Some(ms) = get("RIVERS_LINEAGE_POLL_INTERVAL_MS", None) {
            cfg.poll_interval =
                Duration::from_millis(parse_num("RIVERS_LINEAGE_POLL_INTERVAL_MS", &ms)?);
        }
        if let Some(ms) = get("RIVERS_LINEAGE_SETTLE_MS", None) {
            cfg.settle = Duration::from_millis(parse_num("RIVERS_LINEAGE_SETTLE_MS", &ms)?);
        }
        if let Some(n) = get("RIVERS_LINEAGE_BATCH_SIZE", None) {
            cfg.batch_size = parse_num::<usize>("RIVERS_LINEAGE_BATCH_SIZE", &n)?.max(1);
        }
        Ok(Some(cfg))
    }

    /// The single-event endpoint URL.
    pub fn endpoint_url(&self) -> anyhow::Result<Url> {
        Ok(self.url.join(&self.endpoint)?)
    }

    /// The job namespace for `code_location_id`.
    pub fn job_namespace(&self, code_location_id: &str) -> String {
        self.namespace
            .clone()
            .unwrap_or_else(|| format!("rivers://{code_location_id}"))
    }
}

fn parse_num<T: std::str::FromStr>(key: &str, value: &str) -> anyhow::Result<T> {
    value
        .parse()
        .map_err(|_| anyhow::anyhow!("{key} must be a non-negative integer, got {value:?}"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn from(vars: &[(&str, &str)]) -> anyhow::Result<Option<LineageConfig>> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        LineageConfig::from_lookup(|k| vars.get(k).cloned())
    }

    #[test]
    fn off_without_a_url() {
        assert!(from(&[]).unwrap().is_none());
        assert!(from(&[("RIVERS_LINEAGE_URL", "")]).unwrap().is_none());
    }

    #[test]
    fn enabled_false_turns_it_off() {
        let cfg = from(&[
            ("RIVERS_LINEAGE_URL", "http://hw:8091"),
            ("RIVERS_LINEAGE_ENABLED", "false"),
        ])
        .unwrap();
        assert!(cfg.is_none());
    }

    #[test]
    fn rivers_vars_win_over_openlineage_ones() {
        let cfg = from(&[
            ("RIVERS_LINEAGE_URL", "http://hw:8091"),
            ("OPENLINEAGE_URL", "http://other:1"),
            ("OPENLINEAGE_API_KEY", "tok"),
            ("RIVERS_LINEAGE_NAMESPACE", "analytics"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(cfg.url.as_str(), "http://hw:8091/");
        assert_eq!(cfg.api_key.as_deref(), Some("tok"));
        assert_eq!(cfg.job_namespace("cl"), "analytics");
        assert_eq!(
            cfg.endpoint_url().unwrap().as_str(),
            "http://hw:8091/api/v1/lineage"
        );
    }

    #[test]
    fn openlineage_url_alone_enables_it() {
        let cfg = from(&[("OPENLINEAGE_URL", "http://hw:8091")])
            .unwrap()
            .unwrap();
        assert_eq!(cfg.job_namespace("analytics"), "rivers://analytics");
    }

    #[test]
    fn rejects_bad_values() {
        assert!(from(&[("RIVERS_LINEAGE_URL", "not a url")]).is_err());
        assert!(
            from(&[
                ("RIVERS_LINEAGE_URL", "http://hw"),
                ("RIVERS_LINEAGE_SETTLE_MS", "soon")
            ])
            .is_err()
        );
    }
}
