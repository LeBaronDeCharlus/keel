use crate::worker;
use keel_controlplane::wire::ServiceProxyEntry;
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Every fixture/spec in this project uses this bridge name (Milestone
/// 14's convention); nothing threads a bridge name through the
/// heartbeat/proxy path, so this is hardcoded the same way the design
/// spec itself assumes it.
const PROXY_BRIDGE: &str = "keel0";

/// How long the accept-loop below sleeps between non-blocking accept
/// polls. `std::net::TcpListener::accept` has no cross-thread cancel, so
/// tearing down a listener needs a poll loop with a stop flag rather than
/// a single blocking `accept()` call.
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How long `handle_connection` below waits on a single `connect()` attempt
/// to a replica before giving up on it and moving to the retry-once
/// fallback. The OS default connect timeout (tens of seconds) would let a
/// black-holed (routable but unresponsive) replica -- exactly what "this
/// replica's node just died" looks like on the wire -- stall the first
/// attempt long enough that the retry never gets a chance to run,
/// defeating the failover this proxy exists for. Kept short enough that a
/// dead replica fails fast, long enough not to false-positive a
/// genuinely-slow-but-alive one under normal load.
const RELAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

pub struct ProxiedService {
    replicas: Arc<Mutex<Vec<SocketAddr>>>,
    stop: Arc<AtomicBool>,
    listener_thread: thread::JoinHandle<()>,
    bridge: String,
    vip: String,
}

fn replica_socket_addrs(entry: &ServiceProxyEntry) -> Vec<SocketAddr> {
    entry
        .replicas
        .iter()
        .filter_map(|r| format!("{}:{}", r.address, entry.port).parse().ok())
        .collect()
}

/// Diffs `desired` (this heartbeat round-trip's service table) against
/// `proxied` (what's currently aliased/listening), mutating `proxied` in
/// place: new services get `add_alias` + a spawned listener, known
/// services get their replica list swapped, and disappeared services get
/// torn down + `remove_alias`. Alias changes go through `commands`
/// (`worker::Command::AddServiceAlias`/`RemoveServiceAlias`) rather than a
/// second, independently-owned `NetManager`, mirroring how
/// `reconcile_routes` already reaches the reconciler's `NetManager` for
/// pod_cidr routes.
pub fn reconcile_services(
    desired: &[ServiceProxyEntry],
    proxied: &mut HashMap<String, ProxiedService>,
    commands: &Sender<worker::Command>,
) {
    let desired_names: std::collections::HashSet<&str> =
        desired.iter().map(|e| e.name.as_str()).collect();

    let gone: Vec<String> = proxied
        .keys()
        .filter(|name| !desired_names.contains(name.as_str()))
        .cloned()
        .collect();
    for name in gone {
        if let Some(service) = proxied.remove(&name) {
            service.stop.store(true, Ordering::Relaxed);
            let _ = service.listener_thread.join();
            let (tx, rx) = std::sync::mpsc::channel();
            if commands
                .send(worker::Command::RemoveServiceAlias(
                    service.bridge.clone(),
                    service.vip.clone(),
                    tx,
                ))
                .is_ok()
            {
                let _ = rx.recv();
            }
        }
    }

    for entry in desired {
        let addrs = replica_socket_addrs(entry);
        match proxied.get(&entry.name) {
            Some(service) => {
                *service.replicas.lock().unwrap() = addrs;
            }
            None => {
                let (tx, rx) = std::sync::mpsc::channel();
                if commands
                    .send(worker::Command::AddServiceAlias(
                        PROXY_BRIDGE.to_string(),
                        entry.vip.clone(),
                        tx,
                    ))
                    .is_ok()
                {
                    if let Ok(Err(e)) = rx.recv() {
                        eprintln!("keel-agentd: failed to alias VIP {} on {PROXY_BRIDGE} for service '{}': {e}", entry.vip, entry.name);
                        continue;
                    }
                }

                let listener = match TcpListener::bind(format!("{}:{}", entry.vip, entry.port)) {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("keel-agentd: failed to bind proxy listener for service '{}' on {}:{}: {e}", entry.name, entry.vip, entry.port);
                        continue;
                    }
                };
                listener
                    .set_nonblocking(true)
                    .expect("set_nonblocking never fails on a freshly bound listener");

                let replicas = Arc::new(Mutex::new(addrs));
                let stop = Arc::new(AtomicBool::new(false));
                let counter = Arc::new(AtomicUsize::new(0));

                let thread_replicas = Arc::clone(&replicas);
                let thread_stop = Arc::clone(&stop);
                let thread_counter = Arc::clone(&counter);
                let listener_thread = thread::spawn(move || {
                    accept_loop(listener, thread_replicas, thread_counter, thread_stop)
                });

                proxied.insert(
                    entry.name.clone(),
                    ProxiedService {
                        replicas,
                        stop,
                        listener_thread,
                        bridge: PROXY_BRIDGE.to_string(),
                        vip: entry.vip.clone(),
                    },
                );
            }
        }
    }
}

fn accept_loop(
    listener: TcpListener,
    replicas: Arc<Mutex<Vec<SocketAddr>>>,
    counter: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                let replicas = Arc::clone(&replicas);
                let counter = Arc::clone(&counter);
                thread::spawn(move || handle_connection(stream, &replicas, &counter));
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_INTERVAL)
            }
            Err(_) => thread::sleep(ACCEPT_POLL_INTERVAL),
        }
    }
}

fn handle_connection(
    mut incoming: TcpStream,
    replicas: &Arc<Mutex<Vec<SocketAddr>>>,
    counter: &Arc<AtomicUsize>,
) {
    let snapshot = replicas.lock().unwrap().clone();
    if snapshot.is_empty() {
        return; // dropping `incoming` closes the connection with no reply.
    }
    let start = counter.fetch_add(1, Ordering::Relaxed);
    let attempts = 2.min(snapshot.len());
    for attempt in 0..attempts {
        let target = snapshot[(start + attempt) % snapshot.len()];
        let Ok(mut outgoing) = TcpStream::connect_timeout(&target, RELAY_CONNECT_TIMEOUT) else {
            continue;
        };
        let Ok(mut incoming_clone) = incoming.try_clone() else {
            return;
        };
        let Ok(mut outgoing_clone) = outgoing.try_clone() else {
            return;
        };
        let to_replica = thread::spawn(move || {
            let _ = std::io::copy(&mut incoming_clone, &mut outgoing_clone);
        });
        let _ = std::io::copy(&mut outgoing, &mut incoming);
        let _ = to_replica.join();
        return;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker;
    use keel_controlplane::wire::{ServiceProxyEntry, ServiceReplica};
    use keel_jail::{FakeJailRuntime, FakeMountManager};
    use keel_net::FakeNetManager;
    use keel_zfs::FakeZfsManager;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::time::Duration;

    fn test_reconciler(
        name: &str,
    ) -> crate::Reconciler<FakeJailRuntime, FakeZfsManager, FakeNetManager, FakeMountManager> {
        crate::Reconciler::new(
            FakeJailRuntime::new(),
            FakeZfsManager::new(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            std::env::temp_dir().join(format!("keel-agentd-proxy-test-{name}")),
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap()
    }

    fn spawn_test_worker(name: &str) -> mpsc::Sender<worker::Command> {
        worker::spawn(
            test_reconciler(name),
            FakeZfsManager::new(),
            "zroot".to_string(),
            None,
        )
        .1
    }

    // Binds a plain TCP listener standing in for a replica, echoing
    // whatever it reads back to the sender -- the same idiom
    // registration.rs's own tests already use for a fake remote peer.
    //
    // Bound on `::1` (IPv6 loopback) rather than `127.0.0.1`: in
    // production a service's VIP and every one of its replicas share one
    // port number and differ only by address (see `replica_socket_addrs`,
    // which builds every replica's target as `<replica address>:<entry
    // .port>`). These tests' fake VIP listener is bound on `127.0.0.1` at
    // that same port, so a same-address "replica" wouldn't be a distinct
    // peer at all -- it would resolve to the exact socket the VIP itself
    // already owns, and a connection "relayed" there would just loop back
    // into the VIP's own accept loop instead of reaching a real endpoint.
    // `::1` is a second, independent loopback address that needs no
    // interface alias to bind (unlike `127.0.0.2` and friends, which this
    // sandbox's network stack refuses with "Can't assign requested
    // address" -- confirmed directly against `std::net::TcpListener`,
    // not just at the shell level).
    //
    // `port` lets callers pin the exact port `entry.port` will need
    // (tests that go through `reconcile_services`, where the port is
    // fixed ahead of time); pass 0 to let the OS assign one.
    fn spawn_echo_replica(port: u16) -> std::net::SocketAddr {
        let listener = TcpListener::bind(("::1", port)).unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                if let Ok(n) = stream.read(&mut buf) {
                    let _ = stream.write_all(&buf[..n]);
                }
            }
        });
        addr
    }

    fn spawn_refusing_listener(port: u16) -> std::net::SocketAddr {
        // Bind then immediately drop the listener: the port is released,
        // so a subsequent connect attempt to it is refused, standing in
        // for "this replica is down." Also `::1` -- see
        // `spawn_echo_replica` above for why.
        let listener = TcpListener::bind(("::1", port)).unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr
    }

    // `ServiceReplica::address` is a bare IP string that `replica_socket_addrs`
    // concatenates directly with `entry.port` (`format!("{address}:{port}")`);
    // an IPv6 address needs its own brackets to survive that (`[::1]:8080`,
    // not `::1:8080`, which doesn't parse as a `SocketAddr` at all).
    fn bracketed(addr: std::net::SocketAddr) -> String {
        format!("[{}]", addr.ip())
    }

    #[test]
    fn a_new_service_gets_aliased_and_relays_a_connection_to_its_replica() {
        let commands =
            spawn_test_worker("a_new_service_gets_aliased_and_relays_a_connection_to_its_replica");
        let mut proxied = std::collections::HashMap::new();

        // The proxy binds its OWN listener on <vip>:<port>, and every
        // replica's target is also resolved at that same port (see
        // `replica_socket_addrs`) -- so the fake replica below must bind
        // at this exact port too. Ask the OS for a free one first, then
        // feed that exact port into both the entry and the replica.
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let vip_port = probe.local_addr().unwrap().port();
        drop(probe);

        let replica_addr = spawn_echo_replica(vip_port);

        let entry = ServiceProxyEntry {
            name: "web".to_string(),
            vip: "127.0.0.1".to_string(),
            port: vip_port,
            replicas: vec![ServiceReplica {
                name: "web-0".to_string(),
                node: "node-1".to_string(),
                address: bracketed(replica_addr),
            }],
        };

        reconcile_services(&[entry], &mut proxied, &commands);
        assert!(proxied.contains_key("web"));

        // Give the accept-loop thread a moment to actually bind and start polling.
        std::thread::sleep(Duration::from_millis(100));

        let mut client = TcpStream::connect(("127.0.0.1", vip_port))
            .expect("expected the proxy's listener to be bound");
        client.write_all(b"ping").unwrap();
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"ping");
    }

    #[test]
    fn a_failed_first_replica_retries_the_next_one() {
        // Unlike the other three tests, this one drives `handle_connection`
        // directly instead of going through `reconcile_services` + a real
        // VIP listener. Reason: faithfully reproducing "a dead first
        // replica, a live second replica" through `reconcile_services`
        // needs THREE simultaneously-distinct, real local endpoints (the
        // VIP, the dead replica, the live replica), since a service's VIP
        // and every one of its replicas share one port number and differ
        // only by address (`replica_socket_addrs`) -- a replica sharing
        // the VIP's own address would resolve to the exact socket the VIP
        // already owns rather than being a genuine, independently
        // failing/succeeding peer. This sandbox provides exactly two safe,
        // independently-bindable loopback addresses (`127.0.0.1`, `::1`;
        // `127.0.0.2` and other IPv6/v4 alternates all fail to bind here
        // with "Can't assign requested address", confirmed directly
        // against `std::net::TcpListener`), one short of the three this
        // test needs end-to-end.
        //
        // `handle_connection` itself has no such constraint: it takes an
        // explicit replica list, so it can be given two real, distinct
        // `::1` sockets directly (one bound-then-dropped so it refuses
        // immediately, one running the same echo server the other tests
        // use), still exercising genuine TCP connect failure/success and
        // real byte relay -- just entered one layer below the VIP
        // listener, which the other three tests already cover.
        let dead_addr = spawn_refusing_listener(0);
        let live_addr = spawn_echo_replica(0);

        let replicas = Arc::new(Mutex::new(vec![dead_addr, live_addr]));
        let counter = Arc::new(AtomicUsize::new(0));

        // A plain harness listener standing in for what `accept_loop`
        // would normally hand `handle_connection`: something that gives us
        // a real, already-accepted `TcpStream` on the "incoming" side.
        let harness = TcpListener::bind("127.0.0.1:0").unwrap();
        let harness_addr = harness.local_addr().unwrap();
        let client_thread = std::thread::spawn(move || {
            let mut client = TcpStream::connect(harness_addr).unwrap();
            client.write_all(b"ping").unwrap();
            let mut buf = [0u8; 4];
            client.read_exact(&mut buf).unwrap();
            buf
        });
        let (incoming, _) = harness.accept().unwrap();

        handle_connection(incoming, &replicas, &counter);

        let buf = client_thread.join().unwrap();
        assert_eq!(&buf, b"ping");
    }

    #[test]
    fn a_black_holed_first_replica_still_fails_over_quickly() {
        // Unlike `spawn_refusing_listener` (fast RST -- "replica actively
        // down") this simulates the scenario `RELAY_CONNECT_TIMEOUT`
        // actually exists for: a replica whose node just died, so nothing
        // answers the SYN at all -- no RST, no SYN-ACK. `10.255.255.1` is
        // a private address nothing in this sandbox is configured to
        // answer: confirmed directly (outside this test, via a plain
        // socket connect with a manual timeout) that connecting to it
        // blocks for the full timeout given rather than failing fast, the
        // same black-hole behavior a dead node's last-known address would
        // show on the wire. This is as close to a true network black hole
        // as this sandbox can safely construct without root/firewall
        // control.
        let black_hole: SocketAddr = "10.255.255.1:9".parse().unwrap();
        let live_addr = spawn_echo_replica(0);

        let replicas = Arc::new(Mutex::new(vec![black_hole, live_addr]));
        let counter = Arc::new(AtomicUsize::new(0));

        let harness = TcpListener::bind("127.0.0.1:0").unwrap();
        let harness_addr = harness.local_addr().unwrap();
        let client_thread = std::thread::spawn(move || {
            let mut client = TcpStream::connect(harness_addr).unwrap();
            client.write_all(b"ping").unwrap();
            let mut buf = [0u8; 4];
            client.read_exact(&mut buf).unwrap();
            buf
        });
        let (incoming, _) = harness.accept().unwrap();

        let start = std::time::Instant::now();
        handle_connection(incoming, &replicas, &counter);
        let elapsed = start.elapsed();

        let buf = client_thread.join().unwrap();
        assert_eq!(
            &buf, b"ping",
            "expected failover to the live second replica to still succeed"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "expected RELAY_CONNECT_TIMEOUT to bound the black-holed first attempt well under the OS's ~75s default connect timeout, took {elapsed:?}"
        );
    }

    #[test]
    fn a_service_with_no_replicas_refuses_the_connection_immediately() {
        let commands =
            spawn_test_worker("a_service_with_no_replicas_refuses_the_connection_immediately");
        let mut proxied = std::collections::HashMap::new();

        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let vip_port = probe.local_addr().unwrap().port();
        drop(probe);

        let entry = ServiceProxyEntry {
            name: "web".to_string(),
            vip: "127.0.0.1".to_string(),
            port: vip_port,
            replicas: vec![],
        };
        reconcile_services(&[entry], &mut proxied, &commands);
        std::thread::sleep(Duration::from_millis(100));

        let mut client = TcpStream::connect(("127.0.0.1", vip_port)).unwrap();
        client.write_all(b"ping").unwrap();
        let mut buf = [0u8; 4];
        // No replica to relay to -> the connection is dropped without ever
        // echoing anything back.
        let result = client.read_exact(&mut buf);
        assert!(
            result.is_err(),
            "expected the connection to be closed without a reply, got: {result:?}"
        );
    }

    #[test]
    fn a_disappeared_service_is_torn_down() {
        let commands = spawn_test_worker("a_disappeared_service_is_torn_down");
        let mut proxied = std::collections::HashMap::new();

        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let vip_port = probe.local_addr().unwrap().port();
        drop(probe);

        let replica_addr = spawn_echo_replica(vip_port);

        let entry = ServiceProxyEntry {
            name: "web".to_string(),
            vip: "127.0.0.1".to_string(),
            port: vip_port,
            replicas: vec![ServiceReplica {
                name: "web-0".to_string(),
                node: "node-1".to_string(),
                address: bracketed(replica_addr),
            }],
        };
        reconcile_services(&[entry], &mut proxied, &commands);
        std::thread::sleep(Duration::from_millis(100));
        assert!(proxied.contains_key("web"));

        reconcile_services(&[], &mut proxied, &commands);
        assert!(!proxied.contains_key("web"));
    }
}
