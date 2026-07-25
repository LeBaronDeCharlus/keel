pub mod error;
pub mod fake;
pub mod process;

pub use error::NetError;
pub use fake::FakeNetManager;
pub use process::ProcessNetManager;

/// Computes the gateway address for a jail's bridge, purely from the
/// jail's own `address` parameter: the network's first host address
/// (`network + 1`), with the same prefix length as `address`.
///
/// For example, a jail addressed `10.0.60.5/24` gets the gateway
/// `10.0.60.1/24`.
pub(crate) fn bridge_gateway(address: &str) -> Result<String, NetError> {
    let net: ipnet::Ipv4Net = address.parse().map_err(|_| NetError::InvalidAddress(address.to_string()))?;
    let gateway_ip = std::net::Ipv4Addr::from(u32::from(net.network()) + 1);
    Ok(format!("{gateway_ip}/{}", net.prefix_len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_gateway_computes_the_networks_first_host_address() {
        assert_eq!(bridge_gateway("10.0.60.5/24").unwrap(), "10.0.60.1/24");
    }

    #[test]
    fn bridge_gateway_on_a_malformed_address_returns_a_typed_error_instead_of_panicking() {
        // `NetManager::attach_jail` is a public trait method: any caller
        // that hasn't gone through `keel_spec::validate_address` first
        // (true only for `Reconciler::apply` today, not something this
        // crate can enforce) must get a typed error back, not crash the
        // whole process.
        assert!(matches!(bridge_gateway("not-an-address"), Err(NetError::InvalidAddress(addr)) if addr == "not-an-address"));
    }
}

pub trait NetManager {
    /// Idempotent: creates the bridge if it doesn't already exist and
    /// brings it up. Never destroys a bridge (there is no corresponding
    /// `destroy_bridge` method), since other jails or host config may
    /// depend on it.
    fn ensure_bridge_exists(&self, bridge: &str) -> Result<(), NetError>;

    /// Requires the bridge to already exist (call `ensure_bridge_exists`
    /// first), fails otherwise. Wires the jail's networking as one
    /// coherent operation (epair creation, bridge attachment, VNET
    /// migration, address configuration).
    ///
    /// Tolerates being retried after a partial failure (e.g. the epair
    /// pair already existing from an interrupted prior attempt), but this
    /// method is not atomic: a failed call may still have created some
    /// resources (epair, bridge membership) that a subsequent
    /// `detach_jail` call would need to clean up.
    fn attach_jail(&self, jail_name: &str, bridge: &str, epair_base: &str, address: &str) -> Result<(), NetError>;

    /// Tears down the epair pair; treats an already-absent pair as success
    /// rather than an error (idempotent), mirroring `keel-jail`'s
    /// `remove_resource_limits`.
    ///
    /// Safe to call while the jail is still running, and should be called
    /// before jail destroy, not after: jail destroy does not clean up an
    /// epair moved into it, it orphans it back to the host.
    fn detach_jail(&self, epair_base: &str) -> Result<(), NetError>;

    /// Adds a route to `subnet` via `gateway_addr` to the host's kernel
    /// routing table. Idempotent: adding a route that already exists in
    /// the table with the same gateway is a no-op success, not an error.
    fn add_route(&self, subnet: &str, gateway_addr: &str) -> Result<(), NetError>;

    /// Removes the route to `subnet` from the host's kernel routing table.
    /// Idempotent: removing a route that isn't present is a no-op success.
    fn remove_route(&self, subnet: &str) -> Result<(), NetError>;

    /// Adds `address` as an additional ("alias") address on `bridge`,
    /// alongside whatever address it already has -- unlike `attach_jail`'s
    /// gateway address (the bridge's *first* address, set via a plain
    /// `ifconfig <bridge> inet <addr>`), a service VIP is always a
    /// *second* address on an already-configured bridge, requiring the
    /// `alias` keyword. The alias is always installed with an explicit
    /// `/32` (host) netmask, since a VIP is always a single host address,
    /// never a subnet -- FreeBSD's `ifconfig alias` with no explicit
    /// netmask falls back to the address's legacy classful default (a
    /// `/8` for a `10.x.x.x` VIP), which would install a connected route
    /// wide enough to shadow Milestone 14's per-node pod-CIDR routing.
    /// Idempotent: aliasing an address already present is a no-op
    /// success.
    fn add_alias(&self, bridge: &str, address: &str) -> Result<(), NetError>;

    /// Removes `address` from `bridge`'s aliased addresses. Idempotent:
    /// removing an address that isn't currently aliased is a no-op success.
    fn remove_alias(&self, bridge: &str, address: &str) -> Result<(), NetError>;
}
