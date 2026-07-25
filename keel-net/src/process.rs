use crate::NetError;
use crate::NetManager;
use std::process::{Command, Output};

pub struct ProcessNetManager;

impl ProcessNetManager {
    pub fn new() -> Self {
        Self
    }

    fn run(program: &str, args: &[&str]) -> Result<Output, NetError> {
        Command::new(program)
            .args(args)
            .output()
            .map_err(|e| NetError::Spawn(program.to_string(), e))
    }

    fn run_checked(program: &str, args: &[&str]) -> Result<(), NetError> {
        let output = Self::run(program, args)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(NetError::CommandFailed(
                format!("{program} {}", args.join(" ")),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }

    fn stderr_contains(output: &Output, needle: &str) -> bool {
        String::from_utf8_lossy(&output.stderr).contains(needle)
    }
}

impl Default for ProcessNetManager {
    fn default() -> Self {
        Self::new()
    }
}

impl NetManager for ProcessNetManager {
    fn ensure_bridge_exists(&self, bridge: &str) -> Result<(), NetError> {
        let check = Self::run("ifconfig", &[bridge])?;
        if check.status.success() {
            return Ok(());
        }
        let created = Self::run("ifconfig", &["bridge", "create"])?;
        if !created.status.success() {
            return Err(NetError::CommandFailed(
                "ifconfig bridge create".to_string(),
                created.status,
                String::from_utf8_lossy(&created.stderr).into_owned(),
            ));
        }
        let created_name = String::from_utf8_lossy(&created.stdout).trim().to_string();
        Self::run_checked("ifconfig", &[&created_name, "name", bridge])?;
        Self::run_checked("ifconfig", &[bridge, "up"])
    }

    fn attach_jail(&self, jail_name: &str, bridge: &str, epair_base: &str, address: &str) -> Result<(), NetError> {
        let bridge_check = Self::run("ifconfig", &[bridge])?;
        if !bridge_check.status.success() {
            return Err(NetError::NotFound(bridge.to_string()));
        }

        // `alias`, not a plain `inet` set: a bridge on a single node can now
        // carry more than one distinct subnet's gateway at once (the node's
        // own pod-CIDR, plus the Milestone 21 singleton ingress jail's own
        // fixed `10.0.0.0/24`). A non-aliased `ifconfig <bridge> inet <addr>`
        // *replaces* the bridge's primary address rather than adding a
        // second one alongside it, so attaching a jail from a different
        // subnet than whatever was already primary silently evicted the
        // other subnet's gateway — reproduced directly during Milestone 21
        // VM verification: applying an `Ingress` (address `10.0.0.2/24`)
        // after a `Service` replica (address `10.0.131.2/24`) knocked the
        // Service's own gateway off the bridge, breaking every existing
        // Service on the node. `bridge_gateway`'s returned string already
        // carries an explicit prefix length, so `alias` here needs no
        // separate netmask handling the way `add_alias`'s VIP case does.
        let gateway = crate::bridge_gateway(address)?;
        let gateway_set = Self::run("ifconfig", &[bridge, "inet", &gateway, "alias"])?;
        if !gateway_set.status.success() && !Self::stderr_contains(&gateway_set, "File exists") {
            return Err(NetError::CommandFailed(
                format!("ifconfig {bridge} inet {gateway} alias"),
                gateway_set.status,
                String::from_utf8_lossy(&gateway_set.stderr).into_owned(),
            ));
        }

        let epair_a = format!("{epair_base}a");
        let epair_b = format!("{epair_base}b");

        let create = Self::run("ifconfig", &[epair_base, "create"])?;
        if !create.status.success() && !Self::stderr_contains(&create, "already exists") {
            return Err(NetError::CommandFailed(
                format!("ifconfig {epair_base} create"),
                create.status,
                String::from_utf8_lossy(&create.stderr).into_owned(),
            ));
        }

        let addm = Self::run("ifconfig", &[bridge, "addm", &epair_a])?;
        if !addm.status.success() && !Self::stderr_contains(&addm, "File exists") {
            return Err(NetError::CommandFailed(
                format!("ifconfig {bridge} addm {epair_a}"),
                addm.status,
                String::from_utf8_lossy(&addm.stderr).into_owned(),
            ));
        }

        Self::run_checked("ifconfig", &[&epair_a, "up"])?;

        let vnet_move = Self::run("ifconfig", &[&epair_b, "vnet", jail_name])?;
        if !vnet_move.status.success() {
            // Might already be moved from an interrupted prior attempt —
            // check whether it's already correctly placed in the target jail.
            let already_there = Self::run("jexec", &[jail_name, "/sbin/ifconfig", &epair_b])?;
            if !already_there.status.success() {
                return Err(NetError::CommandFailed(
                    format!("ifconfig {epair_b} vnet {jail_name}"),
                    vnet_move.status,
                    String::from_utf8_lossy(&vnet_move.stderr).into_owned(),
                ));
            }
        }

        Self::run_checked("jexec", &[jail_name, "/sbin/ifconfig", &epair_b, "inet", address])?;
        Self::run_checked("jexec", &[jail_name, "/sbin/ifconfig", &epair_b, "up"])?;

        let gateway_ip = gateway.split('/').next().expect("gateway string always contains '/'");
        let route_add = Self::run("jexec", &[jail_name, "/sbin/route", "add", "default", gateway_ip])?;
        if route_add.status.success() || Self::stderr_contains(&route_add, "File exists") {
            Ok(())
        } else {
            Err(NetError::CommandFailed(
                format!("jexec {jail_name} /sbin/route add default {gateway_ip}"),
                route_add.status,
                String::from_utf8_lossy(&route_add.stderr).into_owned(),
            ))
        }
    }

    fn detach_jail(&self, epair_base: &str) -> Result<(), NetError> {
        let epair_a = format!("{epair_base}a");
        let output = Self::run("ifconfig", &[&epair_a, "destroy"])?;
        if output.status.success() || Self::stderr_contains(&output, "does not exist") {
            Ok(())
        } else {
            Err(NetError::CommandFailed(
                format!("ifconfig {epair_a} destroy"),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }

    fn add_route(&self, subnet: &str, gateway_addr: &str) -> Result<(), NetError> {
        let output = Self::run("route", &["add", "-net", subnet, gateway_addr])?;
        if output.status.success() || Self::stderr_contains(&output, "File exists") {
            Ok(())
        } else {
            Err(NetError::CommandFailed(
                format!("route add -net {subnet} {gateway_addr}"),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }

    fn remove_route(&self, subnet: &str) -> Result<(), NetError> {
        let output = Self::run("route", &["delete", "-net", subnet])?;
        // FreeBSD 15.0's `route(8)` reports a missing route as "route has
        // not been found", not "not in table" - discovered directly on the
        // real FreeBSD VPS during Milestone 21 verification. Checking for
        // both keeps this tolerant of whichever wording an older FreeBSD
        // release this project might still run against uses.
        if output.status.success() || Self::stderr_contains(&output, "not in table") || Self::stderr_contains(&output, "has not been found") {
            Ok(())
        } else {
            Err(NetError::CommandFailed(
                format!("route delete -net {subnet}"),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }

    fn add_alias(&self, bridge: &str, address: &str) -> Result<(), NetError> {
        // A service VIP is always a single host address, never a subnet.
        // FreeBSD's `ifconfig alias` with no explicit netmask falls back to
        // the address's classful default (a bare `alias` of a `10.x.x.x`
        // VIP would install a connected `/8` route, which would
        // shadow/hijack Milestone 14's per-node `10.0.x.0/24` pod-CIDR
        // routing on this node), so the netmask must always be pinned to
        // `/32` explicitly here.
        let netmask = "255.255.255.255";
        let output = Self::run("ifconfig", &[bridge, "alias", address, "netmask", netmask])?;
        if output.status.success() || Self::stderr_contains(&output, "File exists") {
            Ok(())
        } else {
            Err(NetError::CommandFailed(
                format!("ifconfig {bridge} alias {address} netmask {netmask}"),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }

    fn remove_alias(&self, bridge: &str, address: &str) -> Result<(), NetError> {
        let output = Self::run("ifconfig", &[bridge, "-alias", address])?;
        if output.status.success() || Self::stderr_contains(&output, "Can't assign requested address") {
            Ok(())
        } else {
            Err(NetError::CommandFailed(
                format!("ifconfig {bridge} -alias {address}"),
                output.status,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }
}
