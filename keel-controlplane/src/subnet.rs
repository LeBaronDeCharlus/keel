use ipnet::Ipv4Net;
use std::net::Ipv4Addr;

const POD_PREFIX_LEN: u8 = 24;

pub fn derive_pod_cidr(node_id: &str, cluster_cidr: &Ipv4Net) -> Ipv4Net {
    assert!(
        cluster_cidr.prefix_len() <= POD_PREFIX_LEN,
        "cluster CIDR prefix length {} must be <= {POD_PREFIX_LEN} to contain at least one /24 block",
        cluster_cidr.prefix_len()
    );
    let block_count: u32 = 1u32 << (POD_PREFIX_LEN - cluster_cidr.prefix_len());
    let index = fnv1a(node_id.as_bytes()) % block_count;
    let base = u32::from(cluster_cidr.network());
    let block_addr = Ipv4Addr::from(base + index * (1u32 << (32 - POD_PREFIX_LEN)));
    Ipv4Net::new(block_addr, POD_PREFIX_LEN)
        .expect("prefix length 24 is always valid for an IPv4 address")
}

/// Allocates a service's VIP at host-address granularity within
/// `service_cidr` - distinct from `derive_pod_cidr` above, which is
/// hardcoded to /24-block granularity and cannot produce a single host
/// address. `attempt` is the linear-probe offset: `0` is the base hash
/// candidate; the caller (`Services::apply`) increments it on a collision,
/// wrapping around the CIDR's host count until a free address is found or
/// every address has been tried.
pub fn derive_service_vip(service_name: &str, service_cidr: &Ipv4Net, attempt: u32) -> Ipv4Addr {
    assert!(
        service_cidr.prefix_len() > 0,
        "service CIDR prefix length {} must be > 0 to avoid a shift overflow computing its host count",
        service_cidr.prefix_len()
    );
    let host_count: u32 = 1u32 << (32 - service_cidr.prefix_len());
    let index = fnv1a(service_name.as_bytes()).wrapping_add(attempt) % host_count;
    let base = u32::from(service_cidr.network());
    Ipv4Addr::from(base + index)
}

fn fnv1a(bytes: &[u8]) -> u32 {
    const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;
    const FNV_PRIME: u32 = 0x0100_0193;
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cidr(s: &str) -> Ipv4Net {
        s.parse().unwrap()
    }

    #[test]
    fn deterministic_across_repeated_calls() {
        let cluster_cidr = cidr("10.0.0.0/16");
        assert_eq!(
            derive_pod_cidr("node-1", &cluster_cidr),
            derive_pod_cidr("node-1", &cluster_cidr)
        );
    }

    #[test]
    fn matches_hand_computed_fnv1a_values() {
        // fnv1a("node-1") = 1422144387 % 256 = 131; fnv1a("node-2") = 1438922006 % 256 = 22;
        // fnv1a("node-3") = 1455699625 % 256 = 169 (computed independently in Python, see the plan doc).
        let cluster_cidr = cidr("10.0.0.0/16");
        assert_eq!(
            derive_pod_cidr("node-1", &cluster_cidr),
            cidr("10.0.131.0/24")
        );
        assert_eq!(
            derive_pod_cidr("node-2", &cluster_cidr),
            cidr("10.0.22.0/24")
        );
        assert_eq!(
            derive_pod_cidr("node-3", &cluster_cidr),
            cidr("10.0.169.0/24")
        );
    }

    #[test]
    fn different_node_ids_spread_across_the_available_blocks() {
        let cluster_cidr = cidr("10.0.0.0/16");
        let blocks: std::collections::HashSet<Ipv4Net> = (1..=20)
            .map(|i| derive_pod_cidr(&format!("node-{i}"), &cluster_cidr))
            .collect();
        assert!(
            blocks.len() > 15,
            "expected most of 20 node-ids on distinct blocks, got {}",
            blocks.len()
        );
    }

    #[test]
    fn two_node_ids_can_collide_on_a_small_cluster_cidr() {
        // fnv1a("node-4") % 4 == fnv1a("node-8") % 4 == 0 (computed independently in Python).
        let cluster_cidr = cidr("10.0.0.0/22");
        assert_eq!(
            derive_pod_cidr("node-4", &cluster_cidr),
            derive_pod_cidr("node-8", &cluster_cidr)
        );
        assert_eq!(
            derive_pod_cidr("node-4", &cluster_cidr),
            cidr("10.0.0.0/24")
        );
    }

    #[test]
    #[should_panic(expected = "must be <= 24")]
    fn panics_if_cluster_cidr_is_smaller_than_a_single_pod_block() {
        derive_pod_cidr("node-1", &cidr("10.0.0.0/28"));
    }

    #[test]
    #[should_panic(expected = "must be > 0")]
    fn panics_if_service_cidr_prefix_length_is_zero() {
        derive_service_vip("web", &cidr("0.0.0.0/0"), 0);
    }

    #[test]
    fn derive_service_vip_is_deterministic() {
        let service_cidr = cidr("10.0.250.0/24");
        assert_eq!(
            derive_service_vip("web", &service_cidr, 0),
            derive_service_vip("web", &service_cidr, 0)
        );
    }

    #[test]
    fn derive_service_vip_stays_within_the_cidr() {
        let service_cidr = cidr("10.0.250.0/24");
        for name in ["web", "api", "cache", "worker", "db"] {
            let vip = derive_service_vip(name, &service_cidr, 0);
            assert!(
                service_cidr.contains(&vip),
                "{vip} not inside {service_cidr}"
            );
        }
    }

    #[test]
    fn derive_service_vip_produces_a_host_address_not_a_block_address() {
        // The whole point of this function existing separately from
        // derive_pod_cidr: a /24 service_cidr must be able to produce a
        // candidate that does NOT end in .0 (derive_pod_cidr, hardcoded to
        // /24-block granularity, could only ever return service_cidr's own
        // network address here).
        let service_cidr = cidr("10.0.250.0/24");
        let vips: std::collections::HashSet<Ipv4Addr> = (0u32..20)
            .map(|i| derive_service_vip(&format!("svc-{i}"), &service_cidr, 0))
            .collect();
        assert!(
            vips.len() > 1,
            "expected distinct host addresses across 20 service names, got {vips:?}"
        );
    }

    #[test]
    fn derive_service_vip_probing_wraps_around_the_cidr() {
        let service_cidr = cidr("10.0.250.0/30"); // 4 host addresses total
        let base = derive_service_vip("web", &service_cidr, 0);
        let wrapped = derive_service_vip("web", &service_cidr, 4); // one full lap
        assert_eq!(base, wrapped);
    }
}
