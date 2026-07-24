use crate::snapshot::{NodeSnapshot, ServiceSnapshot, Snapshot};

/// Must be kept in sync with `keel-agentd::reconciler::reconcile_certs`'s
/// own `RENEWAL_THRESHOLD_SECS` - this project duplicates the constant
/// rather than sharing it, exactly like its TLS-loading code is duplicated
/// per binary (see `keel-dashboard/src/tls.rs`'s doc comment).
const RENEWAL_THRESHOLD_SECS: i64 = 30 * 24 * 60 * 60;

/// Escapes a string for safe interpolation into HTML text/attribute
/// contexts. Every dynamic string value rendered by this module (node
/// ids/addresses, jail names, service names/VIPs, replica placement
/// strings, volume names, ingress hosts/backend names) is expected to
/// have passed `keel-spec` validation before it ever reaches here, but
/// this escaping is cheap insurance against that upstream invariant
/// rather than a load-bearing security boundary.
fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

pub fn render(snapshot: &Snapshot, now_unix: i64) -> String {
    format!(
        "<!doctype html>\n<html><head><title>keel dashboard</title>\
         <style>body{{font-family:sans-serif;margin:2em}}table{{border-collapse:collapse;margin-bottom:2em}}\
         td,th{{border:1px solid #ccc;padding:4px 8px;text-align:left}}.stale{{background:#fee;padding:8px;margin-bottom:1em}}\
         </style></head><body>\n\
         {stale_banner}<h1>keel dashboard</h1>\n{nodes}\n{jails}\n{services}\n{volumes}\n{ingress}\n\
         <script>setInterval(function(){{fetch('/api/snapshot').then(function(){{location.reload();}}).catch(function(){{}});}}, 5000);</script>\n\
         </body></html>\n",
        stale_banner = render_stale_banner(snapshot, now_unix),
        nodes = render_nodes(&snapshot.nodes),
        jails = render_jails(&snapshot.nodes),
        services = render_services(&snapshot.services),
        volumes = render_volumes(&snapshot.nodes),
        ingress = render_ingress(&snapshot.nodes, now_unix),
    )
}

fn render_stale_banner(snapshot: &Snapshot, now_unix: i64) -> String {
    if !snapshot.stale {
        return String::new();
    }
    let as_of = snapshot.stale_as_of_unix.unwrap_or(now_unix);
    format!("<div class=\"stale\">stale as of unix time {as_of}: control plane unreachable, showing the last known state</div>")
}

fn render_nodes(nodes: &[NodeSnapshot]) -> String {
    let mut rows = String::new();
    for node in nodes {
        let s = &node.status;
        rows.push_str(&format!(
            "<tr><td>{id}</td><td>{addr}</td><td>{status:?}</td><td>{committed_cpu:.2}/{capacity_cpu:.2}</td>\
             <td>{committed_memory}/{capacity_memory}</td><td>{last_seen_secs}s</td>{stale}</tr>",
            id = escape_html(&s.id),
            addr = escape_html(&s.addr),
            status = s.status,
            committed_cpu = s.committed_cpu,
            capacity_cpu = s.capacity_cpu,
            committed_memory = s.committed_memory,
            capacity_memory = s.capacity_memory,
            last_seen_secs = s.last_seen_secs,
            stale = if node.data_stale { "<td>stale</td>" } else { "<td></td>" },
        ));
    }
    format!(
        "<h2>Nodes</h2><table><tr><th>ID</th><th>Address</th><th>Status</th><th>CPU (committed/capacity)</th>\
         <th>Memory (committed/capacity)</th><th>Last seen</th><th></th></tr>{rows}</table>"
    )
}

fn render_jails(nodes: &[NodeSnapshot]) -> String {
    let mut rows = String::new();
    for node in nodes {
        for jail in &node.jails {
            let crash_looping = !jail.running && jail.backoff.current_delay_secs.is_some();
            let state = if jail.running {
                "running"
            } else if crash_looping {
                "crash-looping"
            } else {
                "not running"
            };
            rows.push_str(&format!(
                "<tr><td>{node_id}</td><td>{name}</td><td>{state}</td></tr>",
                node_id = escape_html(&node.status.id),
                name = escape_html(&jail.record.spec.metadata.name),
                state = state,
            ));
        }
    }
    format!("<h2>Jails</h2><table><tr><th>Node</th><th>Name</th><th>State</th></tr>{rows}</table>")
}

fn render_volumes(nodes: &[NodeSnapshot]) -> String {
    let mut rows = String::new();
    for node in nodes {
        for volume in &node.volumes {
            rows.push_str(&format!(
                "<tr><td>{node_id}</td><td>{name}</td></tr>",
                node_id = escape_html(&node.status.id),
                name = escape_html(&volume.name)
            ));
        }
    }
    format!("<h2>Volumes</h2><table><tr><th>Node</th><th>Name</th></tr>{rows}</table>")
}

fn render_services(services: &[ServiceSnapshot]) -> String {
    let mut rows = String::new();
    for service in services {
        let s = &service.summary;
        let replica_placement = service
            .detail
            .as_ref()
            .map(|replicas| {
                replicas
                    .iter()
                    .map(|r| format!("{} ({}@{})", escape_html(&r.name), escape_html(&r.node), escape_html(&r.address)))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let actual_replicas = service.detail.as_ref().map(|replicas| replicas.len()).unwrap_or(0);
        rows.push_str(&format!(
            "<tr><td>{name}</td><td>{actual}/{desired}</td><td>{vip}:{port}</td><td>{placement}</td>{stale}</tr>",
            name = escape_html(&s.name),
            actual = actual_replicas,
            desired = s.desired_replicas,
            vip = escape_html(&s.vip),
            port = s.port,
            placement = replica_placement,
            stale = if service.data_stale { "<td>stale</td>" } else { "<td></td>" },
        ));
    }
    format!(
        "<h2>Services</h2><table><tr><th>Name</th><th>Replicas (actual/desired)</th><th>VIP:Port</th>\
         <th>Placement</th><th></th></tr>{rows}</table>"
    )
}

fn render_ingress(nodes: &[NodeSnapshot], now_unix: i64) -> String {
    let mut rows = String::new();
    for node in nodes {
        for ingress in &node.status.ingresses {
            let expiry_cell = match ingress.cert_expires_at_unix {
                Some(expires_at) if expires_at - now_unix < RENEWAL_THRESHOLD_SECS => {
                    format!(
                        "<td class=\"expiry-warning\" style=\"color:#a00;font-weight:bold\">{expires_at} (renewing soon)</td>"
                    )
                }
                Some(expires_at) => format!("<td>{expires_at}</td>"),
                None => "<td>none</td>".to_string(),
            };
            rows.push_str(&format!(
                "<tr><td>{host}</td><td>{backend_service}:{backend_port}</td>{expiry_cell}</tr>",
                host = escape_html(&ingress.host),
                backend_service = escape_html(&ingress.backend_service),
                backend_port = ingress.backend_port,
                expiry_cell = expiry_cell,
            ));
        }
    }
    format!("<h2>Ingress</h2><table><tr><th>Host</th><th>Backend</th><th>Cert expiry (unix)</th></tr>{rows}</table>")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{NodeSnapshot, ServiceSnapshot};
    use keel_agentd::wire::VolumeStatus;
    use keel_agentd::{BackoffStatus, JailStatus};
    use keel_controlplane::wire::{IngressHealth, NodeState, NodeStatus, ServiceReplica, ServiceSummary};

    fn sample_node_status() -> NodeStatus {
        NodeStatus {
            id: "node-1".to_string(),
            addr: "192.168.64.4:7621".to_string(),
            pod_cidr: "10.0.4.0/24".to_string(),
            status: NodeState::Alive,
            last_seen_secs: 3,
            capacity_cpu: 4.0,
            capacity_memory: 8 * 1024 * 1024 * 1024,
            committed_cpu: 1.5,
            committed_memory: 1024 * 1024 * 1024,
            ingresses: vec![IngressHealth {
                name: "blog".to_string(),
                host: "example.com".to_string(),
                backend_service: "hugo-site".to_string(),
                backend_port: 8080,
                cert_expires_at_unix: Some(1_000_000_000 + 10 * 24 * 60 * 60),
            }],
        }
    }

    fn jail_record(name: &str) -> keel_agentd::JailRecord {
        keel_agentd::JailRecord {
            spec: keel_spec::JailSpec {
                api_version: "keel/v1".to_string(),
                kind: "Jail".to_string(),
                metadata: keel_spec::Metadata { name: name.to_string() },
                spec: keel_spec::Spec {
                    image: "base/14.2-web".to_string(),
                    command: vec!["/usr/local/bin/myapp".to_string()],
                    network: keel_spec::NetworkSpec { vnet: true, bridge: "keel0".to_string(), address: "10.0.0.5/24".to_string() },
                    resources: keel_spec::ResourcesSpec { cpu: "1".to_string(), memory: "512M".to_string() },
                    restart_policy: keel_spec::RestartPolicy::Always,
                    volumes: vec![],
                    replicate_to: None,
                },
            },
            epair_ordinal: 0,
            deleting: false,
        }
    }

    #[test]
    fn renders_a_running_jail_as_running() {
        let node = NodeSnapshot {
            status: sample_node_status(),
            jails: vec![JailStatus { record: jail_record("web-0"), running: true, backoff: BackoffStatus::default() }],
            volumes: vec![],
            data_stale: false,
        };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("web-0"), "got: {html}");
        assert!(html.contains("running"), "got: {html}");
    }

    #[test]
    fn renders_a_backed_off_non_running_jail_as_crash_looping() {
        let node = NodeSnapshot {
            status: sample_node_status(),
            jails: vec![JailStatus {
                record: jail_record("web-0"),
                running: false,
                backoff: BackoffStatus { retry_in_secs: Some(4), current_delay_secs: Some(8) },
            }],
            volumes: vec![],
            data_stale: false,
        };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("crash-looping"), "got: {html}");
    }

    #[test]
    fn renders_a_freshly_applied_non_running_jail_as_not_running_not_crash_looping() {
        let node = NodeSnapshot {
            status: sample_node_status(),
            jails: vec![JailStatus { record: jail_record("web-0"), running: false, backoff: BackoffStatus::default() }],
            volumes: vec![],
            data_stale: false,
        };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(!html.contains("crash-looping"), "got: {html}");
    }

    #[test]
    fn renders_a_volume_name_grouped_under_its_node() {
        let node = NodeSnapshot {
            status: sample_node_status(),
            jails: vec![],
            volumes: vec![VolumeStatus { name: "web-data".to_string() }],
            data_stale: false,
        };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("web-data"), "got: {html}");
    }

    #[test]
    fn renders_a_service_with_desired_and_actual_replica_counts() {
        let service = ServiceSnapshot {
            summary: ServiceSummary { name: "web".to_string(), desired_replicas: 3, vip: "10.0.250.7".to_string(), port: 8080 },
            detail: Some(vec![ServiceReplica {
                name: "web-0".to_string(),
                node: "node-1".to_string(),
                address: "10.0.4.5".to_string(),
            }]),
            data_stale: false,
        };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![], services: vec![service], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("10.0.250.7"), "got: {html}");
        assert!(html.contains("web-0"), "got: {html}");
    }

    #[test]
    fn renders_a_cert_expiry_warning_inside_the_thirty_day_threshold() {
        let mut node_status = sample_node_status();
        node_status.ingresses[0].cert_expires_at_unix = Some(1_000_000_000 + 20 * 24 * 60 * 60);
        let node = NodeSnapshot { status: node_status, jails: vec![], volumes: vec![], data_stale: false };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("expiry-warning"), "got: {html}");
    }

    #[test]
    fn does_not_render_a_cert_expiry_warning_outside_the_threshold() {
        let mut node_status = sample_node_status();
        node_status.ingresses[0].cert_expires_at_unix = Some(1_000_000_000 + 60 * 24 * 60 * 60);
        let node = NodeSnapshot { status: node_status, jails: vec![], volumes: vec![], data_stale: false };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(!html.contains("expiry-warning"), "got: {html}");
    }

    #[test]
    fn renders_a_stale_banner_when_the_snapshot_is_stale() {
        let snapshot = crate::snapshot::Snapshot { nodes: vec![], services: vec![], stale: true, stale_as_of_unix: Some(1_000_000_000) };
        let html = render(&snapshot, 1_000_000_500);
        assert!(html.contains("control plane unreachable"), "got: {html}");
    }

    #[test]
    fn does_not_render_a_stale_banner_when_the_snapshot_is_fresh() {
        let snapshot = crate::snapshot::Snapshot { nodes: vec![], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(!html.contains("control plane unreachable"), "got: {html}");
    }

    #[test]
    fn renders_a_stale_marker_cell_for_a_stale_node() {
        let node = NodeSnapshot { status: sample_node_status(), jails: vec![], volumes: vec![], data_stale: true };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("<td>stale</td>"), "got: {html}");
    }

    #[test]
    fn renders_a_stale_marker_cell_for_a_stale_service() {
        let service = ServiceSnapshot {
            summary: ServiceSummary { name: "web".to_string(), desired_replicas: 3, vip: "10.0.250.7".to_string(), port: 8080 },
            detail: None,
            data_stale: true,
        };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![], services: vec![service], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("<td>stale</td>"), "got: {html}");
    }

    #[test]
    fn renders_none_for_an_ingress_with_no_cert_expiry() {
        let mut node_status = sample_node_status();
        node_status.ingresses[0].cert_expires_at_unix = None;
        let node = NodeSnapshot { status: node_status, jails: vec![], volumes: vec![], data_stale: false };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("<td>none</td>"), "got: {html}");
    }

    #[test]
    fn escapes_html_special_characters_in_dynamic_values() {
        let mut node_status = sample_node_status();
        node_status.id = "<script>alert(1)</script>".to_string();
        node_status.ingresses[0].host = "a & b".to_string();
        let node = NodeSnapshot { status: node_status, jails: vec![], volumes: vec![], data_stale: false };
        let snapshot = crate::snapshot::Snapshot { nodes: vec![node], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(!html.contains("<script>alert(1)</script>"), "got: {html}");
        assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"), "got: {html}");
        assert!(html.contains("a &amp; b"), "got: {html}");
    }

    #[test]
    fn renders_a_js_fetch_poll_loop_instead_of_a_meta_refresh() {
        let snapshot = crate::snapshot::Snapshot { nodes: vec![], services: vec![], stale: false, stale_as_of_unix: None };
        let html = render(&snapshot, 1_000_000_000);
        assert!(html.contains("fetch('/api/snapshot')"), "got: {html}");
        assert!(!html.contains("http-equiv=\"refresh\""), "got: {html}");
    }
}
