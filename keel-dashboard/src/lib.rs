//! `keel-dashboard`: a read-only web dashboard for cluster state. An mTLS
//! client of `keel-controlplane` (polling into an in-memory `Snapshot`) and
//! its own TLS-terminating HTTP server for browsers.

pub mod control_plane_client;
pub mod html;
pub mod http;
pub mod poller;
pub mod snapshot;
pub mod tls;
