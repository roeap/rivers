//! OpenLineage emission for rivers.
//!
//! A per-code-location [`LineageEmitter`] tails the storage event log and
//! turns runs, asset steps and materializations into OpenLineage events (see
//! [`translate`] for the job and dataset model). It runs inside the rivers
//! daemon, is configured by [`LineageConfig`], and delivers over
//! [`HttpTransport`].

pub mod config;
pub mod emitter;
pub mod ids;
pub mod metadata;
pub mod translate;
pub mod transport;

pub use config::LineageConfig;
pub use emitter::LineageEmitter;
pub use transport::HttpTransport;

/// Re-export so callers can name events and transports without their own dep.
pub use openlineage_client;
