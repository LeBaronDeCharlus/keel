//! `keel-dashboard`: a read-only web dashboard for cluster state. An mTLS
//! client of `keel-controlplane` (polling into an in-memory `Snapshot`) and
//! its own Basic-Auth-protected, TLS-terminating HTTP server for browsers.
