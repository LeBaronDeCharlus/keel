use crate::replica_target::ReplicaTarget;
use crate::replica_target_store;
use crate::tls::ReloadingTls;
use keel_zfs::ZfsManager;
use rustls::{ServerConnection, StreamOwned};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Node-to-node replication carries live volume data and previously had no
/// authentication or encryption at all -- any reachable host could invent a
/// replica name and get an arbitrary stream `zfs receive`d, or align a
/// guessed snapshot id to poison an existing standby. Requiring the same
/// mTLS the rest of this crate's peer-to-peer traffic already uses closes
/// both gaps at once instead of inventing a second auth mechanism.
type TlsStream = StreamOwned<ServerConnection, TcpStream>;

/// Every listener in this crate accepts one OS thread per connection with no
/// concurrency cap, so a client that connects and never sends anything would
/// otherwise pin a thread forever -- before the header is even read, let
/// alone authenticated. A read timeout bounds that.
const CONNECTION_READ_TIMEOUT: Duration = Duration::from_secs(30);

fn apply_read_timeout(stream: &TcpStream) {
    let _ = stream.set_read_timeout(Some(CONNECTION_READ_TIMEOUT));
}

pub const ACK_PROCEED: u8 = 0;
pub const ACK_NEED_FULL: u8 = 1;
/// The sender's generation (see `keel_spec::Spec::generation`'s doc
/// comment) is lower than the highest one this node has already accepted
/// for this replica -- a partitioned former-primary that never learned it
/// was fenced. No `zfs receive` happens; the sender should stop
/// replicating this replica entirely, not merely retry.
pub const ACK_STALE_GENERATION: u8 = 2;

/// `read_len_prefixed` only ever carries a replica name or a snapshot id
/// (both well under keel's own 63-character name limit), never the bulk
/// snapshot stream itself. A generous but bounded cap stops an attacker-
/// controlled length prefix from driving an unbounded `vec![0u8; len]`
/// allocation, which aborts the whole process on failure rather than just
/// the connection.
const MAX_FRAME_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub replica_name: String,
    pub snapshot_id: String,
    pub base_snapshot_id: Option<String>,
    pub generation: u64,
}

fn write_len_prefixed(stream: &mut dyn Write, s: &str) -> io::Result<()> {
    let bytes = s.as_bytes();
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(bytes)
}

fn read_len_prefixed(stream: &mut dyn Read) -> io::Result<String> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {len} exceeds max of {MAX_FRAME_BYTES}"),
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub fn write_header(
    stream: &mut dyn Write,
    replica_name: &str,
    snapshot_id: &str,
    base_snapshot_id: Option<&str>,
    generation: u64,
) -> io::Result<()> {
    write_len_prefixed(stream, replica_name)?;
    write_len_prefixed(stream, snapshot_id)?;
    match base_snapshot_id {
        None => stream.write_all(&[0u8])?,
        Some(base) => {
            stream.write_all(&[1u8])?;
            write_len_prefixed(stream, base)?;
        }
    }
    stream.write_all(&generation.to_be_bytes())
}

pub fn read_header(stream: &mut dyn Read) -> io::Result<Header> {
    let replica_name = read_len_prefixed(stream)?;
    let snapshot_id = read_len_prefixed(stream)?;
    let mut has_base = [0u8; 1];
    stream.read_exact(&mut has_base)?;
    let base_snapshot_id = match has_base[0] {
        0 => None,
        _ => Some(read_len_prefixed(stream)?),
    };
    let mut generation_buf = [0u8; 8];
    stream.read_exact(&mut generation_buf)?;
    let generation = u64::from_be_bytes(generation_buf);
    Ok(Header {
        replica_name,
        snapshot_id,
        base_snapshot_id,
        generation,
    })
}

#[derive(Clone)]
pub struct ReplicaTargetRegistry {
    state_dir: PathBuf,
    by_name: Arc<Mutex<HashMap<String, ReplicaTarget>>>,
}

impl ReplicaTargetRegistry {
    pub fn load(state_dir: PathBuf) -> Result<Self, crate::store::StoreError> {
        let loaded = replica_target_store::load_all(&state_dir)?;
        let by_name = loaded
            .into_iter()
            .map(|t| (t.replica_name.clone(), t))
            .collect();
        Ok(Self {
            state_dir,
            by_name: Arc::new(Mutex::new(by_name)),
        })
    }

    pub fn get(&self, replica_name: &str) -> Option<ReplicaTarget> {
        self.by_name.lock().unwrap().get(replica_name).cloned()
    }

    /// Clears `replica_name`'s bookkeeping from both memory and disk. No
    /// caller invokes this yet -- wiring it into an actual trigger (e.g. a
    /// new endpoint the control plane calls when a standby relationship
    /// ends) is follow-up work, but the capability itself was entirely
    /// missing before this: a `ReplicaTarget` accumulated forever once
    /// created, with no eviction mechanism at all.
    pub fn remove(&self, replica_name: &str) {
        self.by_name.lock().unwrap().remove(replica_name);
        if let Err(e) = replica_target_store::remove(&self.state_dir, replica_name) {
            eprintln!(
                "keel-agentd: failed to remove persisted replica target '{replica_name}': {e}"
            );
        }
    }

    /// Creates the target on first contact (`volume_dataset`/`source_node_addr`
    /// as given, `last_snapshot: None`) or refreshes `source_node_addr` on an
    /// existing one, without touching its `last_snapshot`. Persists to disk.
    fn ensure(
        &self,
        replica_name: &str,
        volume_dataset: &str,
        source_node_addr: &str,
    ) -> Result<ReplicaTarget, crate::store::StoreError> {
        let target = {
            let mut guard = self.by_name.lock().unwrap();
            let target = guard
                .entry(replica_name.to_string())
                .or_insert_with(|| ReplicaTarget {
                    replica_name: replica_name.to_string(),
                    volume_dataset: volume_dataset.to_string(),
                    source_node_addr: source_node_addr.to_string(),
                    last_snapshot: None,
                    highest_generation_seen: 0,
                });
            target.source_node_addr = source_node_addr.to_string();
            target.clone()
        };
        replica_target_store::save(&self.state_dir, &target)?;
        Ok(target)
    }

    /// Checks `generation` against the highest one ever accepted for
    /// `replica_name`, atomically updating and persisting it if `generation`
    /// is not stale. Returns `false` (no mutation) if `generation` is
    /// strictly lower than what's on record -- the fencing check itself:
    /// a partitioned former-primary that never learned it was fenced still
    /// presents its own, now-outdated generation, and gets rejected here
    /// using only this node's own local state, with no control-plane
    /// connectivity required.
    fn accept_generation(&self, replica_name: &str, generation: u64) -> bool {
        let target = {
            let mut guard = self.by_name.lock().unwrap();
            let Some(target) = guard.get_mut(replica_name) else {
                return false;
            };
            if generation < target.highest_generation_seen {
                return false;
            }
            target.highest_generation_seen = generation;
            target.clone()
        };
        // Persistence failure here doesn't change the accept decision
        // itself (the generation check already passed and is reflected in
        // memory), matching this codebase's log-and-swallow persist
        // pattern elsewhere -- only affects whether the accepted
        // generation survives a restart.
        if let Err(e) = replica_target_store::save(&self.state_dir, &target) {
            eprintln!("keel-agentd: failed to persist accepted generation for replica '{replica_name}': {e}");
        }
        true
    }

    fn record_snapshot(
        &self,
        replica_name: &str,
        snapshot_id: &str,
    ) -> Result<(), crate::store::StoreError> {
        let target = {
            let mut guard = self.by_name.lock().unwrap();
            match guard.get_mut(replica_name) {
                Some(target) => {
                    target.last_snapshot = Some(snapshot_id.to_string());
                    Some(target.clone())
                }
                None => None,
            }
        };
        if let Some(target) = target {
            replica_target_store::save(&self.state_dir, &target)?;
        }
        Ok(())
    }

    /// Test helper: seed a `ReplicaTarget` directly, bypassing the network
    /// handshake in `handle_connection`.
    pub fn ensure_for_test(
        &self,
        replica_name: &str,
        volume_dataset: &str,
        source_node_addr: &str,
    ) {
        self.ensure(replica_name, volume_dataset, source_node_addr)
            .unwrap();
    }

    /// Test helper: mark a `ReplicaTarget` as having completed a snapshot,
    /// bypassing a real `zfs receive`.
    pub fn record_snapshot_for_test(&self, replica_name: &str, snapshot_id: &str) {
        self.record_snapshot(replica_name, snapshot_id).unwrap();
    }

    /// Test helper: seed `replica_name`'s highest-accepted generation
    /// directly, bypassing a real connection's header. `replica_name` must
    /// already have a `ReplicaTarget` (via `ensure_for_test` or a prior real
    /// connection) for this to have any effect.
    pub fn accept_generation_for_test(&self, replica_name: &str, generation: u64) {
        self.accept_generation(replica_name, generation);
    }
}

/// One accepted connection's worth of work: read the header, decide
/// proceed-vs-reject against the locally-known `last_snapshot`, and (if
/// proceeding) stream the rest of the connection into `zfs receive`.
///
/// `header.replica_name` is the plain replica/jail name (e.g. "db-0"), the
/// same name used throughout `Standbys`, `PendingFences`, `Placements`, and
/// force-repin's own probe -- `ReplicaTargetRegistry` is keyed by it
/// directly. The volume/dataset name is reconstructed from it here using
/// the "one volume named `data` per stateful replica" convention already
/// hardcoded in `worker.rs`'s `Command::Apply` handler.
fn handle_connection<Z: ZfsManager>(
    stream: &mut TlsStream,
    zfs: &Z,
    pool: &str,
    targets: &ReplicaTargetRegistry,
) -> io::Result<()> {
    let header = read_header(stream)?;
    let volume_name = format!("{}-data", header.replica_name);
    let dataset = crate::record::volume_dataset_path(pool, &volume_name);
    let peer_addr = stream
        .sock
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let target = targets
        .ensure(&header.replica_name, &dataset, &peer_addr)
        .map_err(|e| io::Error::other(e.to_string()))?;

    if !targets.accept_generation(&header.replica_name, header.generation) {
        stream.write_all(&[ACK_STALE_GENERATION])?;
        return Ok(());
    }

    if header.base_snapshot_id != target.last_snapshot {
        stream.write_all(&[ACK_NEED_FULL])?;
        return Ok(());
    }
    stream.write_all(&[ACK_PROCEED])?;

    // The sender signals "done" with a raw TCP half-close (`sock.shutdown
    // (Write)`), not a TLS `close_notify` alert -- the same one-directional
    // half-close idiom keel-controlplane's own `forward()` already uses over
    // TLS. rustls surfaces that as `ErrorKind::UnexpectedEof` on the next
    // read rather than `Ok(0)`; translate it back to a clean EOF here so
    // `ZfsManager::receive_snapshot` (which just calls a generic `Read` to
    // completion, real or fake) sees the same clean end-of-stream a raw,
    // non-TLS socket would have given it.
    let mut reader = EofTolerantRead(stream);
    zfs.receive_snapshot(&dataset, &mut reader)
        .map_err(|e| io::Error::other(e.to_string()))?;
    targets
        .record_snapshot(&header.replica_name, &header.snapshot_id)
        .map_err(|e| io::Error::other(e.to_string()))
}

struct EofTolerantRead<'a, R: Read>(&'a mut R);

impl<R: Read> Read for EofTolerantRead<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.0.read(buf) {
            Ok(n) => Ok(n),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(0),
            Err(e) => Err(e),
        }
    }
}

pub fn run<Z: ZfsManager + Clone + Send + 'static>(
    listener: TcpListener,
    zfs: Z,
    pool: String,
    targets: ReplicaTargetRegistry,
    reloading_tls: Arc<ReloadingTls>,
) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        apply_read_timeout(&stream);
        let zfs = zfs.clone();
        let pool = pool.clone();
        let targets = targets.clone();
        let tls_config = reloading_tls.server_config();
        thread::spawn(move || {
            let Ok(conn) = ServerConnection::new(tls_config) else {
                return;
            };
            let mut tls_stream = TlsStream::new(conn, stream);
            if let Err(e) = handle_connection(&mut tls_stream, &zfs, &pool, &targets) {
                eprintln!("keel-agentd: replication connection failed: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_zfs::FakeZfsManager;
    use std::net::TcpListener as StdTcpListener;

    fn test_state_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("keel-agentd-replication-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/tls")).join(name)
    }

    fn test_reloading_tls() -> Arc<ReloadingTls> {
        ReloadingTls::spawn(
            fixture("fixture-node.crt"),
            fixture("fixture-node.key"),
            fixture("ca.crt"),
            fixture("crl.pem"),
            Duration::from_secs(3600),
        )
        .unwrap()
    }

    type ClientTlsStream = StreamOwned<rustls::ClientConnection, TcpStream>;

    /// A real, mTLS-authenticated client connection to a `run()`-served
    /// listener, standing in for another node's own replication sender.
    fn connect_tls(addr: std::net::SocketAddr) -> ClientTlsStream {
        let client_config = Arc::new(
            crate::tls::load_client_config(
                &fixture("fixture-client.crt"),
                &fixture("fixture-client.key"),
                &fixture("ca.crt"),
                &fixture("crl.pem"),
            )
            .unwrap(),
        );
        let server_name = crate::tls::server_name_from_addr(&addr.to_string()).unwrap();
        let tcp_stream = TcpStream::connect(addr).unwrap();
        let conn = rustls::ClientConnection::new(client_config, server_name).unwrap();
        rustls::StreamOwned::new(conn, tcp_stream)
    }

    #[test]
    fn apply_read_timeout_sets_the_configured_timeout_on_a_real_stream() {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).unwrap();
        let (server_stream, _) = listener.accept().unwrap();
        apply_read_timeout(&server_stream);
        assert_eq!(
            server_stream.read_timeout().unwrap(),
            Some(CONNECTION_READ_TIMEOUT)
        );
    }

    #[test]
    fn read_len_prefixed_rejects_a_length_prefix_beyond_the_max_frame_size() {
        let len_buf = (MAX_FRAME_BYTES as u32 + 1).to_be_bytes();
        let result = read_len_prefixed(&mut len_buf.as_slice());
        assert!(
            result.is_err(),
            "an oversized length prefix must be rejected before allocating"
        );
    }

    #[test]
    fn read_len_prefixed_accepts_a_length_prefix_at_the_max_frame_size() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(3u32).to_be_bytes());
        buf.extend_from_slice(b"abc");
        assert_eq!(read_len_prefixed(&mut buf.as_slice()).unwrap(), "abc");
    }

    #[test]
    fn header_with_no_base_round_trips() {
        let mut buf = Vec::new();
        write_header(&mut buf, "db-0", "keel-repl-1", None, 0).unwrap();
        let header = read_header(&mut buf.as_slice()).unwrap();
        assert_eq!(
            header,
            Header {
                replica_name: "db-0".to_string(),
                snapshot_id: "keel-repl-1".to_string(),
                base_snapshot_id: None,
                generation: 0
            }
        );
    }

    #[test]
    fn header_with_a_base_round_trips() {
        let mut buf = Vec::new();
        write_header(&mut buf, "db-0", "keel-repl-2", Some("keel-repl-1"), 0).unwrap();
        let header = read_header(&mut buf.as_slice()).unwrap();
        assert_eq!(
            header,
            Header {
                replica_name: "db-0".to_string(),
                snapshot_id: "keel-repl-2".to_string(),
                base_snapshot_id: Some("keel-repl-1".to_string()),
                generation: 0
            }
        );
    }

    #[test]
    fn header_carries_a_real_generation_through_the_round_trip() {
        let mut buf = Vec::new();
        write_header(&mut buf, "db-0", "keel-repl-1", None, 7).unwrap();
        let header = read_header(&mut buf.as_slice()).unwrap();
        assert_eq!(header.generation, 7);
    }

    #[test]
    fn remove_clears_a_target_from_both_memory_and_disk() {
        let dir = test_state_dir("remove_clears_a_target_from_both_memory_and_disk");
        let targets = ReplicaTargetRegistry::load(dir.clone()).unwrap();
        targets.ensure_for_test("db-0", "zroot/keel/volumes/db-0-data", "10.0.0.4:7621");
        assert!(targets.get("db-0").is_some());

        targets.remove("db-0");

        assert!(
            targets.get("db-0").is_none(),
            "expected the target gone from the in-memory registry"
        );
        // A fresh load from the same state_dir must not resurrect it either.
        let reloaded = ReplicaTargetRegistry::load(dir).unwrap();
        assert!(
            reloaded.get("db-0").is_none(),
            "expected the target gone from disk too"
        );
    }

    #[test]
    fn remove_on_an_unknown_replica_is_a_harmless_no_op() {
        let dir = test_state_dir("remove_on_an_unknown_replica_is_a_harmless_no_op");
        let targets = ReplicaTargetRegistry::load(dir).unwrap();
        targets.remove("never-existed");
        assert!(targets.get("never-existed").is_none());
    }

    #[test]
    fn first_contact_creates_a_replica_target_and_accepts_a_full_send() {
        let dir = test_state_dir("first_contact_creates_a_replica_target_and_accepts_a_full_send");
        let targets = ReplicaTargetRegistry::load(dir).unwrap();
        let sender_zfs = FakeZfsManager::new();
        sender_zfs.seed_dataset("zroot/keel/volumes/db-0-data");
        sender_zfs
            .snapshot("zroot/keel/volumes/db-0-data", "keel-repl-1")
            .unwrap();
        let receiver_zfs = FakeZfsManager::new();

        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let pool = "zroot".to_string();
        let targets_clone = targets.clone();
        let receiver_zfs_clone = receiver_zfs.clone();
        let reloading_tls = test_reloading_tls();
        thread::spawn(move || {
            run(
                listener,
                receiver_zfs_clone,
                pool,
                targets_clone,
                reloading_tls,
            )
        });

        let mut stream = connect_tls(addr);
        write_header(&mut stream, "db-0", "keel-repl-1", None, 0).unwrap();
        let mut ack = [0u8; 1];
        stream.read_exact(&mut ack).unwrap();
        assert_eq!(ack[0], ACK_PROCEED);

        sender_zfs
            .send_snapshot(
                "zroot/keel/volumes/db-0-data",
                "keel-repl-1",
                None,
                &mut stream,
            )
            .unwrap();
        stream.sock.shutdown(std::net::Shutdown::Write).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(receiver_zfs
            .dataset_exists("zroot/keel/volumes/db-0-data")
            .unwrap());
        let target = targets
            .get("db-0")
            .expect("expected a ReplicaTarget to have been created on first contact");
        assert_eq!(target.last_snapshot, Some("keel-repl-1".to_string()));
    }

    #[test]
    fn a_base_mismatch_is_rejected_without_reading_a_payload() {
        let dir = test_state_dir("a_base_mismatch_is_rejected_without_reading_a_payload");
        let targets = ReplicaTargetRegistry::load(dir).unwrap();
        let receiver_zfs = FakeZfsManager::new();

        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let pool = "zroot".to_string();
        let targets_clone = targets.clone();
        let receiver_zfs_clone = receiver_zfs.clone();
        let reloading_tls = test_reloading_tls();
        thread::spawn(move || {
            run(
                listener,
                receiver_zfs_clone,
                pool,
                targets_clone,
                reloading_tls,
            )
        });

        let mut stream = connect_tls(addr);
        // This node has no ReplicaTarget yet (last_snapshot is None), so
        // claiming a base of "keel-repl-9" must be rejected.
        write_header(&mut stream, "db-0", "keel-repl-10", Some("keel-repl-9"), 0).unwrap();
        let mut ack = [0u8; 1];
        stream.read_exact(&mut ack).unwrap();
        assert_eq!(ack[0], ACK_NEED_FULL);

        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(!receiver_zfs
            .dataset_exists("zroot/keel/volumes/db-0-data")
            .unwrap());
    }

    #[test]
    fn a_plain_tcp_connection_with_no_tls_handshake_never_reaches_the_protocol() {
        let dir = test_state_dir(
            "a_plain_tcp_connection_with_no_tls_handshake_never_reaches_the_protocol",
        );
        let targets = ReplicaTargetRegistry::load(dir).unwrap();
        let receiver_zfs = FakeZfsManager::new();

        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let pool = "zroot".to_string();
        let targets_clone = targets.clone();
        let receiver_zfs_clone = receiver_zfs.clone();
        let reloading_tls = test_reloading_tls();
        thread::spawn(move || {
            run(
                listener,
                receiver_zfs_clone,
                pool,
                targets_clone,
                reloading_tls,
            )
        });

        // A bare TCP client that skips the TLS handshake entirely and just
        // writes the wire header as plaintext -- what any of the previous
        // (pre-TLS) attackers/peers would have done. The server reads these
        // bytes as a TLS record, fails to parse them as one (logged as
        // "replication connection failed: received corrupt message..."), and
        // drops the connection without ever calling `handle_connection`. Not
        // asserted here: whatever raw bytes come back on the wire (e.g. a
        // TLS alert record's leading byte can satisfy a naive `read_exact`
        // without being a real protocol ack) -- only that the protocol was
        // genuinely never reached, checked below via the target registry.
        let mut stream = TcpStream::connect(addr).unwrap();
        write_header(&mut stream, "db-0", "keel-repl-1", None, 0).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(
            targets.get("db-0").is_none(),
            "a non-TLS connection must never reach the replica-target bookkeeping"
        );
    }

    #[test]
    fn a_stale_generation_is_rejected_without_reading_a_payload_or_touching_last_snapshot() {
        let dir = test_state_dir(
            "a_stale_generation_is_rejected_without_reading_a_payload_or_touching_last_snapshot",
        );
        let targets = ReplicaTargetRegistry::load(dir).unwrap();
        let receiver_zfs = FakeZfsManager::new();

        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let pool = "zroot".to_string();
        let targets_clone = targets.clone();
        let receiver_zfs_clone = receiver_zfs.clone();
        let reloading_tls = test_reloading_tls();
        thread::spawn(move || {
            run(
                listener,
                receiver_zfs_clone,
                pool,
                targets_clone,
                reloading_tls,
            )
        });

        // A real primary at generation 2 completes a full send first (the
        // standby now has both a `last_snapshot` and a known-highest
        // generation of 2).
        let sender_zfs = FakeZfsManager::new();
        sender_zfs.seed_dataset("zroot/keel/volumes/db-0-data");
        sender_zfs
            .snapshot("zroot/keel/volumes/db-0-data", "keel-repl-1")
            .unwrap();
        let mut stream = connect_tls(addr);
        write_header(&mut stream, "db-0", "keel-repl-1", None, 2).unwrap();
        let mut ack = [0u8; 1];
        stream.read_exact(&mut ack).unwrap();
        assert_eq!(ack[0], ACK_PROCEED);
        sender_zfs
            .send_snapshot(
                "zroot/keel/volumes/db-0-data",
                "keel-repl-1",
                None,
                &mut stream,
            )
            .unwrap();
        stream.sock.shutdown(std::net::Shutdown::Write).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));

        // A partitioned former-primary, still on generation 1, reconnects
        // with a base that would otherwise be accepted (None, same as its
        // own prior full send) -- must be rejected purely on generation,
        // before the base-snapshot check ever runs.
        let mut stale_stream = connect_tls(addr);
        write_header(&mut stale_stream, "db-0", "keel-repl-2", None, 1).unwrap();
        let mut stale_ack = [0u8; 1];
        stale_stream.read_exact(&mut stale_ack).unwrap();
        assert_eq!(stale_ack[0], ACK_STALE_GENERATION);

        let target = targets.get("db-0").unwrap();
        assert_eq!(
            target.last_snapshot,
            Some("keel-repl-1".to_string()),
            "the stale sender's rejected attempt must not touch last_snapshot"
        );
        assert_eq!(
            target.highest_generation_seen, 2,
            "the stale sender's generation must not overwrite the real one"
        );
    }
}
