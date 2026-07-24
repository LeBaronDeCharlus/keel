use crate::backoff::BackoffState;
use crate::ingress_record::IngressRecord;
use crate::ingress_store;
use crate::record::{self, JailRecord};
use crate::store::{self, StoreError};
use crate::wire::JailStatus;
use keel_jail::{JailRuntime, MountManager};
use keel_net::NetManager;
use keel_spec::{IngressSpec, JailSpec};
use keel_zfs::ZfsManager;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReconcileError {
    #[error("spec validation failed: {0}")]
    InvalidSpec(#[from] keel_spec::SpecError),
    #[error("state store error: {0}")]
    Store(#[from] StoreError),
    #[error("jail runtime error: {0}")]
    Jail(#[from] keel_jail::JailError),
    #[error("zfs error: {0}")]
    Zfs(#[from] keel_zfs::ZfsError),
    #[error("network error: {0}")]
    Net(#[from] keel_net::NetError),
    #[error("mount error: {0}")]
    Mount(#[from] keel_jail::MountError),
    #[error("jail '{0}' not found in desired state")]
    NotFound(String),
    #[error("base image dataset '{0}' does not exist")]
    BaseImageNotFound(String),
}

pub struct Reconciler<J: JailRuntime, Z: ZfsManager, N: NetManager, M: MountManager> {
    jails: J,
    zfs: Z,
    net: N,
    mounts: M,
    pool: String,
    state_dir: PathBuf,
    records: HashMap<String, JailRecord>,
    backoff: HashMap<String, BackoffState>,
    next_epair_ordinal: u32,
    ingress_records: HashMap<String, IngressRecord>,
    ingress_backoff: HashMap<String, BackoffState>,
    acme: Box<dyn keel_ingress::AcmeClient + Send>,
    dns: Box<dyn keel_ingress::DnsProvider + Send>,
    nginx: Box<dyn crate::nginx::NginxController + Send>,
    service_vips: crate::ServiceVipSlot,
}

impl<J: JailRuntime, Z: ZfsManager, N: NetManager, M: MountManager> Reconciler<J, Z, N, M> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        jails: J,
        zfs: Z,
        net: N,
        mounts: M,
        pool: String,
        state_dir: PathBuf,
        acme: Box<dyn keel_ingress::AcmeClient + Send>,
        dns: Box<dyn keel_ingress::DnsProvider + Send>,
        nginx: Box<dyn crate::nginx::NginxController + Send>,
        service_vips: crate::ServiceVipSlot,
    ) -> Result<Self, ReconcileError> {
        let loaded = store::load_all(&state_dir)?;
        let next_epair_ordinal = loaded.iter().map(|r| r.epair_ordinal).max().map(|m| m + 1).unwrap_or(1);
        let records = loaded.into_iter().map(|r| (r.spec.metadata.name.clone(), r)).collect();
        let ingress_loaded = ingress_store::load_all(&state_dir)?;
        let ingress_records = ingress_loaded.into_iter().map(|r| (r.spec.metadata.name.clone(), r)).collect();
        Ok(Self {
            jails,
            zfs,
            net,
            mounts,
            pool,
            state_dir,
            records,
            backoff: HashMap::new(),
            next_epair_ordinal,
            ingress_records,
            ingress_backoff: HashMap::new(),
            acme,
            dns,
            nginx,
            service_vips,
        })
    }

    pub fn apply(&mut self, spec: JailSpec) -> Result<(), ReconcileError> {
        keel_spec::validate_name(&spec.metadata.name)?;
        keel_spec::validate_image(&spec.spec.image)?;
        keel_spec::validate_bridge(&spec.spec.network.bridge)?;
        keel_spec::validate_address(&spec.spec.network.address)?;
        keel_spec::parse_cpu_cores(&spec.spec.resources.cpu)?;
        keel_spec::parse_memory_bytes(&spec.spec.resources.memory)?;
        keel_spec::validate_volumes(&spec.spec.volumes)?;

        let epair_ordinal = if let Some(existing) = self.records.get(&spec.metadata.name) {
            keel_spec::validate_transition(&existing.spec, &spec)?;
            existing.epair_ordinal
        } else {
            let ordinal = self.next_epair_ordinal;
            self.next_epair_ordinal += 1;
            ordinal
        };

        let record = JailRecord { spec: spec.clone(), epair_ordinal, deleting: false };
        store::save(&self.state_dir, &record)?;
        self.records.insert(spec.metadata.name.clone(), record);
        Ok(())
    }

    pub fn delete(&mut self, name: &str) -> Result<(), ReconcileError> {
        let mut record = self.records.get(name).ok_or_else(|| ReconcileError::NotFound(name.to_string()))?.clone();
        // Persisted before any destructive action below: a crash between
        // here and the final `store::remove` leaves a record on disk that
        // still says "delete me," not "provision me" -- `reconcile_one`
        // resumes (rather than reprovisions) any record it finds marked
        // this way. Skipped if already set, so a retried delete (e.g. after
        // a transient "device busy" failure) doesn't re-persist a no-op
        // write on every attempt.
        if !record.deleting {
            record.deleting = true;
            store::save(&self.state_dir, &record)?;
            self.records.insert(name.to_string(), record.clone());
        }
        let jail_name = record::jail_name(name);
        let epair_base = record::epair_base_name(record.epair_ordinal);
        let jail_dataset = record::jail_dataset_path(&self.pool, name);
        let rootfs = record::jail_rootfs_path(&self.pool, name);

        self.net.detach_jail(&epair_base)?;
        // A record can reach `delete` having only gone through `apply`
        // (never `provision`, e.g. the daemon restarted before its first
        // reconcile pass): `destroy`, `destroy_dataset`, and
        // `remove_resource_limits` are not intrinsically idempotent, so
        // treat their `NotFound` as "already torn down" here.
        match self.jails.destroy(&jail_name) {
            Ok(()) | Err(keel_jail::JailError::NotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        // Unmount every declared volume before destroying the rootfs
        // dataset (avoids the exact "device busy" class of failure this
        // crate's own `destroy_dataset` retry loop was written against for
        // the rootfs mount itself) — but never destroy a volume's own
        // dataset here; that is this milestone's entire "decoupled
        // lifecycle" guarantee.
        for volume in &record.spec.spec.volumes {
            let target = rootfs.join(volume.mount_path.trim_start_matches('/'));
            match self.mounts.unmount(&target) {
                Ok(()) | Err(keel_jail::MountError::NotMounted(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        match self.zfs.destroy_dataset(&jail_dataset) {
            Ok(()) | Err(keel_zfs::ZfsError::NotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        match self.jails.remove_resource_limits(&jail_name) {
            Ok(()) | Err(keel_jail::JailError::NotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }

        store::remove(&self.state_dir, name)?;
        self.records.remove(name);
        self.backoff.remove(name);
        Ok(())
    }

    pub fn add_route(&self, subnet: &str, gateway_addr: &str) -> Result<(), keel_net::NetError> {
        self.net.add_route(subnet, gateway_addr)
    }

    pub fn remove_route(&self, subnet: &str) -> Result<(), keel_net::NetError> {
        self.net.remove_route(subnet)
    }

    pub fn add_alias(&self, bridge: &str, address: &str) -> Result<(), keel_net::NetError> {
        self.net.add_alias(bridge, address)
    }

    pub fn remove_alias(&self, bridge: &str, address: &str) -> Result<(), keel_net::NetError> {
        self.net.remove_alias(bridge, address)
    }

    /// Never consults `self.records` — a volume can outlive every jail
    /// record that ever referenced it, which is this milestone's whole
    /// point. `Ok(())` means the dataset exists; the caller (HTTP layer)
    /// maps that to a 200 with a minimal body.
    pub fn get_volume(&self, name: &str) -> Result<(), ReconcileError> {
        let dataset = record::volume_dataset_path(&self.pool, name);
        if self.zfs.dataset_exists(&dataset)? {
            Ok(())
        } else {
            Err(ReconcileError::Zfs(keel_zfs::ZfsError::NotFound(dataset)))
        }
    }

    pub fn delete_volume(&mut self, name: &str) -> Result<(), ReconcileError> {
        let dataset = record::volume_dataset_path(&self.pool, name);
        self.zfs.destroy_dataset(&dataset)?;
        Ok(())
    }

    /// Every volume on this node, discovered by enumerating ZFS datasets
    /// under `<pool>/keel/volumes/` rather than consulting `self.records` -
    /// see `get_volume`'s doc comment: a volume can outlive every jail
    /// record that ever referenced it, so `self.records` is never a
    /// complete list of volumes.
    pub fn list_volumes(&self) -> Result<Vec<crate::wire::VolumeStatus>, ReconcileError> {
        let parent = format!("{}/keel/volumes", self.pool);
        let prefix = format!("{parent}/");
        let mut names: Vec<String> =
            self.zfs.list_child_datasets(&parent)?.into_iter().map(|dataset| dataset[prefix.len()..].to_string()).collect();
        names.sort();
        Ok(names.into_iter().map(|name| crate::wire::VolumeStatus { name }).collect())
    }

    /// Retargets an *already-running* primary's replication without a full
    /// re-provision -- just updates `replicate_to` on the existing
    /// `JailRecord` and persists it. Bypasses `apply()`/`validate_transition`
    /// entirely: `replicate_to` isn't an immutability-checked field, and this
    /// path has no spec to validate against, only a bare address to store.
    pub fn set_replicate_to(&mut self, name: &str, replicate_to: Option<String>) -> Result<(), ReconcileError> {
        let mut record = self.records.get(name).ok_or_else(|| ReconcileError::NotFound(name.to_string()))?.clone();
        record.spec.spec.replicate_to = replicate_to;
        store::save(&self.state_dir, &record)?;
        self.records.insert(name.to_string(), record);
        Ok(())
    }

    pub fn apply_ingress(&mut self, spec: IngressSpec) -> Result<(), ReconcileError> {
        keel_spec::validate_name(&spec.metadata.name)?;
        keel_spec::validate_host(&spec.spec.host)?;
        keel_spec::validate_email(&spec.spec.tls.email)?;
        if spec.spec.backend.port == 0 {
            return Err(keel_spec::SpecError::InvalidPort(0).into());
        }
        let cert_expires_at_unix = self.ingress_records.get(&spec.metadata.name).and_then(|r| r.cert_expires_at_unix);
        let record = IngressRecord { spec: spec.clone(), cert_expires_at_unix };
        ingress_store::save(&self.state_dir, &record)?;
        self.ingress_records.insert(spec.metadata.name.clone(), record);
        Ok(())
    }

    pub fn get_ingress(&self, name: &str) -> Option<IngressRecord> {
        self.ingress_records.get(name).cloned()
    }

    pub fn list_ingress(&self) -> Vec<IngressRecord> {
        self.ingress_records.values().cloned().collect()
    }

    pub fn delete_ingress(&mut self, name: &str) -> Result<(), ReconcileError> {
        if !self.ingress_records.contains_key(name) {
            return Err(ReconcileError::NotFound(name.to_string()));
        }
        ingress_store::remove(&self.state_dir, name)?;
        self.ingress_records.remove(name);
        self.ingress_backoff.remove(name);
        Ok(())
    }

    fn configure_networking_and_limits(&mut self, name: &str, record: &JailRecord) -> Result<(), ReconcileError> {
        let jail_name = record::jail_name(name);
        let epair_base = record::epair_base_name(record.epair_ordinal);
        self.net.ensure_bridge_exists(&record.spec.spec.network.bridge)?;
        self.net.attach_jail(
            &jail_name,
            &record.spec.spec.network.bridge,
            &epair_base,
            &record.spec.spec.network.address,
        )?;
        let pcpu_percent = keel_spec::cores_to_pcpu_percent(keel_spec::parse_cpu_cores(&record.spec.spec.resources.cpu)?);
        let memory_bytes = keel_spec::parse_memory_bytes(&record.spec.spec.resources.memory)?;
        self.jails.set_resource_limits(&jail_name, pcpu_percent, memory_bytes)?;
        Ok(())
    }

    /// Ensures every volume the spec declares exists and is mounted at its
    /// target -- idempotent, so it's safe to call unconditionally from both
    /// `provision` (a fresh jail) and `restart` (an existing one, possibly
    /// resuming after a crash that happened between `jails.create`
    /// succeeding and this very loop ever running the first time).
    fn ensure_volumes(&mut self, name: &str, record: &JailRecord) -> Result<(), ReconcileError> {
        let rootfs = record::jail_rootfs_path(&self.pool, name);
        for volume in &record.spec.spec.volumes {
            let dataset = record::volume_dataset_path(&self.pool, &volume.name);
            let target = rootfs.join(volume.mount_path.trim_start_matches('/'));
            self.mounts.ensure_mount_point(&target)?;
            if !self.zfs.dataset_exists(&dataset)? {
                self.zfs.create_volume(&dataset, &volume.size)?;
            }
            if !self.mounts.is_mounted(&target)? {
                self.mounts.mount_nullfs(&record::volume_mountpoint(&self.pool, &volume.name), &target)?;
            }
        }
        Ok(())
    }

    fn provision(&mut self, name: &str, record: &JailRecord) -> Result<(), ReconcileError> {
        let jail_name = record::jail_name(name);
        let base_dataset = record::base_dataset_path(&self.pool, &record.spec.spec.image);
        let jail_dataset = record::jail_dataset_path(&self.pool, name);
        let rootfs = record::jail_rootfs_path(&self.pool, name);

        if !self.zfs.dataset_exists(&base_dataset)? {
            return Err(ReconcileError::BaseImageNotFound(base_dataset));
        }
        self.zfs.clone_from_base(&base_dataset, &jail_dataset)?;
        self.jails.create(&jail_name, &rootfs, record.spec.spec.network.vnet)?;
        self.ensure_volumes(name, record)?;
        self.configure_networking_and_limits(name, record)?;
        self.jails.start_command(&jail_name, &record.spec.spec.command)?;
        Ok(())
    }

    fn restart(&mut self, name: &str, record: &JailRecord) -> Result<(), ReconcileError> {
        let jail_name = record::jail_name(name);
        self.ensure_volumes(name, record)?;
        self.configure_networking_and_limits(name, record)?;
        self.jails.start_command(&jail_name, &record.spec.spec.command)?;
        Ok(())
    }

    /// Best-effort cleanup after a failed `provision`. Every call here
    /// discards its `Result` (`let _ =`): `detach_jail` is intrinsically
    /// idempotent, and `destroy`/`destroy_dataset`/`remove_resource_limits`
    /// return `NotFound` for a step that never completed — discarding the
    /// result either way makes it safe to call all four unconditionally
    /// even when only some steps of `provision` actually ran. A rollback
    /// failure for a reason other than "already gone" is handled by the
    /// normal per-jail backoff on the next reconciliation pass, not
    /// specially here.
    ///
    /// Declared volumes are unmounted first, the same ordering `delete()`
    /// already uses: a provision failure that happens after the
    /// volume-mount step would otherwise leave the mount sitting on top of
    /// the rootfs dataset, making `destroy_dataset` fail silently (busy)
    /// and wedging every later retry's `zfs clone` on "already exists".
    fn rollback_provision(&mut self, name: &str, record: &JailRecord) {
        let jail_name = record::jail_name(name);
        let epair_base = record::epair_base_name(record.epair_ordinal);
        let jail_dataset = record::jail_dataset_path(&self.pool, name);
        let rootfs = record::jail_rootfs_path(&self.pool, name);
        let _ = self.net.detach_jail(&epair_base);
        let _ = self.jails.destroy(&jail_name);
        for volume in &record.spec.spec.volumes {
            let target = rootfs.join(volume.mount_path.trim_start_matches('/'));
            let _ = self.mounts.unmount(&target);
        }
        let _ = self.zfs.destroy_dataset(&jail_dataset);
        let _ = self.jails.remove_resource_limits(&jail_name);
    }

    /// Synthesizes and applies an ordinary `JailSpec` (named `"ingress"`, so
    /// `record::jail_name` turns it into the actual jail name
    /// `"keel-ingress"`) the first time an `Ingress` spec has ever been
    /// applied, reusing `apply`'s existing crash-safe provisioning/rollback
    /// path unchanged. A no-op once the `"ingress"` jail record already
    /// exists, or if no `Ingress` spec has ever been applied.
    fn ensure_ingress_jail(&mut self) -> Result<(), ReconcileError> {
        if self.ingress_records.is_empty() || self.records.contains_key("ingress") {
            return Ok(());
        }
        let spec = keel_spec::JailSpec {
            api_version: "keel/v1".to_string(),
            kind: "Jail".to_string(),
            metadata: keel_spec::Metadata { name: "ingress".to_string() },
            spec: keel_spec::Spec {
                image: "base/keel-ingress".to_string(),
                command: vec![
                    "/usr/local/sbin/nginx".to_string(),
                    "-c".to_string(),
                    crate::nginx::NGINX_CONF_PATH.to_string(),
                    "-g".to_string(),
                    "daemon off;".to_string(),
                ],
                network: keel_spec::NetworkSpec {
                    vnet: true,
                    bridge: "keel0".to_string(),
                    address: format!("{}/24", record::INGRESS_JAIL_BRIDGE_ADDR),
                },
                resources: keel_spec::ResourcesSpec { cpu: "1".to_string(), memory: "256M".to_string() },
                restart_policy: keel_spec::RestartPolicy::Always,
                volumes: vec![],
                replicate_to: None,
            },
        };
        self.apply(spec)
    }

    pub fn reconcile(&mut self, now: Instant) -> Vec<(String, ReconcileError)> {
        if let Err(e) = self.ensure_ingress_jail() {
            eprintln!("keel-agentd: failed to ensure the ingress jail exists: {e}");
        }
        // Provision/restart every tracked jail -- including, on a fresh
        // node's first Ingress apply, the ingress jail itself -- *before*
        // reconcile_certs/reconcile_ingress_config write anything into the
        // ingress jail's rootfs. Those writes assume that rootfs is already
        // a real, cloned ZFS dataset; running them first would create plain
        // directories at a path a real `zfs clone` then has to contend with
        // as its own mountpoint.
        let names: Vec<String> = self.records.keys().cloned().collect();
        let mut failures = Vec::new();
        for name in names {
            if let Err(e) = self.reconcile_one(&name, now) {
                failures.push((name, e));
            }
        }
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        self.reconcile_certs(now_unix);
        self.reconcile_ingress_config();
        failures
    }

    /// Issues or renews a TLS certificate for every tracked `Ingress` whose
    /// certificate is missing or within `RENEWAL_THRESHOLD_SECS` of expiry.
    /// Uses `ingress_backoff` (mirroring `reconcile_one`'s own use of
    /// `backoff` for jail provisioning retries) so that one Ingress stuck in
    /// a failing ACME/DNS loop cannot starve every other Ingress's own
    /// reconcile pass by being retried on every single tick.
    ///
    /// Unlike jail provisioning (where `record_attempt` arms the cooldown
    /// unconditionally, even on success, because a successful `start` gives
    /// no guarantee the process keeps running), a successful certificate
    /// issuance here is fully durable the moment it's written to disk and
    /// persisted -- there's no equivalent "might still fail shortly after
    /// success" risk. So `record_attempt` is only called on a genuine
    /// failure (ACME/DNS, the on-disk write, or persistence), not after
    /// every attempt; a real periodic reconcile loop still won't hammer a
    /// working ACME endpoint since the next issuance for that host is
    /// naturally more than 30 days away.
    ///
    /// Takes a wall-clock `now_unix: i64` rather than folding into
    /// `reconcile`'s own `Instant now` - a certificate's expiry is
    /// inherently wall-clock (unlike everything else this reconciler
    /// tracks), so tests can inject it directly instead of depending on the
    /// real clock.
    fn reconcile_certs(&mut self, now_unix: i64) {
        const RENEWAL_THRESHOLD_SECS: i64 = 30 * 24 * 60 * 60;
        let names: Vec<String> = self.ingress_records.keys().cloned().collect();
        for name in names {
            let can_retry = self.ingress_backoff.entry(name.clone()).or_default().can_retry(Instant::now());
            if !can_retry {
                continue;
            }
            let record = self.ingress_records[&name].clone();
            let cert_file_missing = !self.cert_exists_in_ingress_jail(&record.spec.spec.host);
            let needs_issuance = match record.cert_expires_at_unix {
                None => true,
                // A timestamp alone isn't proof the cert is actually usable:
                // if the ingress jail's rootfs was ever recreated (e.g. lost
                // and re-cloned from the base image after a crash, or torn
                // down and reprovisioned), `cert_expires_at_unix` still
                // claims a valid cert while the file itself is gone from the
                // new rootfs. Reproduced directly during Milestone 21 VM
                // verification: recreating the ingress jail to fix an
                // unrelated base-image issue silently wiped the previously
                // issued cert while the `Ingress` record's own expiry
                // timestamp (persisted separately) still looked fine.
                Some(expires_at) => cert_file_missing || expires_at - now_unix < RENEWAL_THRESHOLD_SECS,
            };
            if !needs_issuance {
                continue;
            }
            match self.acme.request_certificate(&record.spec.spec.host, &record.spec.spec.tls.email, self.dns.as_ref()) {
                Ok(cert) => {
                    if let Err(e) = self.write_cert_to_ingress_jail(&record.spec.spec.host, &cert) {
                        eprintln!("keel-agentd: failed to write certificate for ingress '{name}' into the ingress jail: {e}");
                        self.ingress_backoff.get_mut(&name).unwrap().record_attempt(Instant::now());
                        continue;
                    }
                    let mut updated = record;
                    // Placeholder 90-day validity: `FakeAcmeClient` doesn't
                    // return a real expiry. Task 16's real `AcmeClient` must
                    // parse the actual `notAfter` out of the issued
                    // certificate instead of hardcoding this.
                    updated.cert_expires_at_unix = Some(now_unix + 90 * 24 * 60 * 60);
                    if let Err(e) = ingress_store::save(&self.state_dir, &updated) {
                        eprintln!("keel-agentd: failed to persist certificate expiry for ingress '{name}': {e}");
                        self.ingress_backoff.get_mut(&name).unwrap().record_attempt(Instant::now());
                        continue;
                    }
                    self.ingress_records.insert(name, updated);
                }
                Err(e) => {
                    eprintln!("keel-agentd: certificate issuance failed for ingress '{name}': {e}");
                    self.ingress_backoff.get_mut(&name).unwrap().record_attempt(Instant::now());
                }
            }
        }
    }

    /// Regenerates nginx's config from every applied `Ingress` joined
    /// against `service_vips`'s current `Service` VIP table, then
    /// `write_config` -> `test_config` -> `reload` in sequence. Aborts the
    /// sequence (no reload, and on a `test_config` failure no disruption of
    /// whatever config is currently live) as soon as any step fails. An
    /// `Ingress` whose backend service has no known VIP yet (the control
    /// plane hasn't reported it, or it's simply down) is silently omitted
    /// from this pass's rendered config rather than failing the whole
    /// reconcile; the next tick picks it up once the VIP becomes known.
    fn reconcile_ingress_config(&mut self) {
        let backends: Vec<keel_ingress::IngressBackendConfig> = self
            .ingress_records
            .values()
            .filter_map(|record| {
                let (vip, port) = self.service_vips.get(&record.spec.spec.backend.service)?;
                Some(keel_ingress::IngressBackendConfig {
                    host: record.spec.spec.host.clone(),
                    vip,
                    port,
                    cert_path: format!("/usr/local/etc/nginx/certs/{}.crt", record.spec.spec.host),
                    key_path: format!("/usr/local/etc/nginx/certs/{}.key", record.spec.spec.host),
                })
            })
            .collect();
        let config = keel_ingress::render_nginx_config(&backends);
        if let Err(e) = self.nginx.write_config("keel-ingress", &config) {
            eprintln!("keel-agentd: failed to write ingress nginx config: {e}");
            return;
        }
        if let Err(e) = self.nginx.test_config("keel-ingress") {
            eprintln!("keel-agentd: ingress nginx config failed validation, leaving the previous config live: {e}");
            return;
        }
        if let Err(e) = self.nginx.reload("keel-ingress") {
            eprintln!("keel-agentd: failed to reload ingress nginx: {e}");
        }
    }

    fn cert_exists_in_ingress_jail(&self, host: &str) -> bool {
        let crt_path = record::jail_rootfs_path(&self.pool, "ingress").join("usr/local/etc/nginx/certs").join(format!("{host}.crt"));
        crt_path.is_file()
    }

    fn write_cert_to_ingress_jail(&self, host: &str, cert: &keel_ingress::Cert) -> Result<(), ReconcileError> {
        let certs_dir = record::jail_rootfs_path(&self.pool, "ingress").join("usr/local/etc/nginx/certs");
        std::fs::create_dir_all(&certs_dir).map_err(|e| ReconcileError::Store(StoreError::Io(certs_dir.clone(), e)))?;
        let crt_path = certs_dir.join(format!("{host}.crt"));
        let key_path = certs_dir.join(format!("{host}.key"));
        std::fs::write(&crt_path, &cert.cert_pem).map_err(|e| ReconcileError::Store(StoreError::Io(crt_path, e)))?;
        std::fs::write(&key_path, &cert.key_pem).map_err(|e| ReconcileError::Store(StoreError::Io(key_path, e)))?;
        Ok(())
    }

    fn reconcile_one(&mut self, name: &str, now: Instant) -> Result<(), ReconcileError> {
        let record = self.records[name].clone();
        let jail_name = record::jail_name(name);

        let can_retry = self.backoff.entry(name.to_string()).or_insert_with(BackoffState::new).can_retry(now);
        if !can_retry {
            return Ok(());
        }

        if record.deleting {
            // Resuming a delete left incomplete by a crash (or retrying one
            // that failed, e.g. a transient "device busy"): reuses this
            // name's existing backoff state rather than a separate cadence.
            // Recorded *before* calling `delete`, since a successful delete
            // removes this very backoff entry.
            self.backoff.get_mut(name).unwrap().record_attempt(now);
            return self.delete(name);
        }

        let exists = self.jails.jail_exists(&jail_name)?;

        if !exists {
            let result = self.provision(name, &record);
            self.backoff.get_mut(name).unwrap().record_attempt(now);
            if result.is_err() {
                self.rollback_provision(name, &record);
            }
            result
        } else {
            let running = self.jails.is_running(&jail_name)?;
            if running {
                // The only place genuine uptime is observable — see
                // `BackoffState::note_running`'s own doc comment for why
                // this can't be inferred from attempt timing alone.
                self.backoff.get_mut(name).unwrap().note_running(now);
                let pcpu_percent =
                    keel_spec::cores_to_pcpu_percent(keel_spec::parse_cpu_cores(&record.spec.spec.resources.cpu)?);
                let memory_bytes = keel_spec::parse_memory_bytes(&record.spec.spec.resources.memory)?;
                self.jails.set_resource_limits(&jail_name, pcpu_percent, memory_bytes)?;
                Ok(())
            } else if record.spec.spec.restart_policy == keel_spec::RestartPolicy::Never {
                Ok(())
            } else {
                let result = self.restart(name, &record);
                self.backoff.get_mut(name).unwrap().record_attempt(now);
                result
            }
        }
    }

    pub fn get(&self, name: &str, now: Instant) -> Option<JailStatus> {
        let record = self.records.get(name)?.clone();
        let jail_name = record::jail_name(name);
        // A transient runtime query error is treated as "not confirmed
        // running" rather than failing the whole status read - the spec
        // and backoff info below are still valid and useful on their own.
        let running = self.jails.is_running(&jail_name).unwrap_or(false);
        let backoff = self.backoff.get(name).map(|b| b.status(now)).unwrap_or_default();
        Some(JailStatus { record, running, backoff })
    }

    pub fn list(&self, now: Instant) -> Vec<JailStatus> {
        let mut names: Vec<&String> = self.records.keys().collect();
        names.sort();
        names.iter().filter_map(|name| self.get(name, now)).collect()
    }

    /// Sums resources across every tracked record. Deliberately tolerant of
    /// a record whose `resources` no longer parse: `Reconciler::new` loads
    /// records from disk with no re-validation, so a state file written by
    /// an older/different code version (or a hand-edited/corrupted one)
    /// could otherwise reach this on every heartbeat -- panicking here would
    /// kill the worker thread permanently (the daemon looks alive but never
    /// reconciles again) rather than just that one jail's contribution.
    pub fn committed_resources(&self) -> (f64, u64) {
        self.records.values().fold((0.0, 0u64), |(cpu, mem), record| {
            let parsed = keel_spec::parse_cpu_cores(&record.spec.spec.resources.cpu)
                .and_then(|cpu_cores| Ok((cpu_cores, keel_spec::parse_memory_bytes(&record.spec.spec.resources.memory)?)));
            match parsed {
                Ok((cpu_cores, mem_bytes)) => (cpu + cpu_cores, mem + mem_bytes),
                Err(e) => {
                    eprintln!(
                        "keel-agentd: jail '{}' has unparsable resources, excluding it from committed_resources: {e}",
                        record.spec.metadata.name
                    );
                    (cpu, mem)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_jail::{FakeJailRuntime, FakeMountManager};
    use keel_net::FakeNetManager;
    use keel_spec::{Metadata, NetworkSpec, RestartPolicy, ResourcesSpec, Spec};
    use keel_zfs::FakeZfsManager;
    use std::fs;
    use std::time::Duration;

    fn sample_spec(name: &str) -> JailSpec {
        JailSpec {
            api_version: "keel/v1".to_string(),
            kind: "Jail".to_string(),
            metadata: Metadata { name: name.to_string() },
            spec: Spec {
                image: "base/14.2-web".to_string(),
                command: vec!["/usr/local/bin/myapp".to_string()],
                network: NetworkSpec {
                    vnet: true,
                    bridge: "keel0".to_string(),
                    address: "10.0.0.5/24".to_string(),
                },
                resources: ResourcesSpec { cpu: "2".to_string(), memory: "512M".to_string() },
                restart_policy: RestartPolicy::Always,
                volumes: vec![],
                replicate_to: None,
            },
        }
    }

    fn test_state_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("keel-agentd-reconciler-test-{name}"));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn new_reconciler(state_dir: PathBuf) -> Reconciler<FakeJailRuntime, FakeZfsManager, FakeNetManager, FakeMountManager> {
        Reconciler::new(
            FakeJailRuntime::new(),
            FakeZfsManager::new(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            state_dir,
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap()
    }

    fn sample_spec_with_volume(name: &str, volume_name: &str, mount_path: &str, size: &str) -> JailSpec {
        let mut spec = sample_spec(name);
        spec.spec.volumes = vec![keel_spec::VolumeMount {
            name: volume_name.to_string(),
            mount_path: mount_path.to_string(),
            size: size.to_string(),
        }];
        spec
    }

    #[test]
    fn new_starts_with_no_records_on_an_empty_state_dir() {
        let dir = test_state_dir("new_starts_with_no_records_on_an_empty_state_dir");
        let reconciler = new_reconciler(dir);
        assert!(reconciler.records.is_empty());
        assert_eq!(reconciler.next_epair_ordinal, 1);
    }

    fn sample_spec_with_resources(name: &str, cpu: &str, memory: &str) -> JailSpec {
        let mut spec = sample_spec(name);
        spec.spec.resources = ResourcesSpec { cpu: cpu.to_string(), memory: memory.to_string() };
        spec
    }

    #[test]
    fn committed_resources_on_an_empty_reconciler_is_zero() {
        let dir = test_state_dir("committed_resources_on_an_empty_reconciler_is_zero");
        let reconciler = new_reconciler(dir);
        assert_eq!(reconciler.committed_resources(), (0.0, 0));
    }

    #[test]
    fn committed_resources_sums_across_all_tracked_jails() {
        let dir = test_state_dir("committed_resources_sums_across_all_tracked_jails");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply(sample_spec_with_resources("web-1", "2", "512M")).unwrap();
        reconciler.apply(sample_spec_with_resources("web-2", "1.5", "1G")).unwrap();
        let (cpu, memory) = reconciler.committed_resources();
        assert_eq!(cpu, 3.5);
        assert_eq!(memory, 512 * 1024 * 1024 + 1024 * 1024 * 1024);
    }

    #[test]
    fn committed_resources_skips_a_record_with_an_unparsable_cpu_value_instead_of_panicking() {
        // `Reconciler::new` loads records from disk with no re-validation,
        // so a state file written by an older/different code version (or a
        // hand-edited/corrupted one) that the current parser rejects can
        // reach `committed_resources` -- which is called on every heartbeat.
        // It must skip such a record, not panic the whole worker thread.
        let dir = test_state_dir("committed_resources_skips_a_record_with_an_unparsable_cpu_value");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply(sample_spec_with_resources("web-1", "2", "512M")).unwrap();
        let mut corrupt = reconciler.records["web-1"].clone();
        corrupt.spec.metadata.name = "web-2".to_string();
        corrupt.spec.spec.resources.cpu = "not-a-number".to_string();
        reconciler.records.insert("web-2".to_string(), corrupt);

        let (cpu, memory) = reconciler.committed_resources();
        assert_eq!(cpu, 2.0, "the malformed record must be skipped, not counted or panicked on");
        assert_eq!(memory, 512 * 1024 * 1024);
    }

    #[test]
    fn committed_resources_drops_a_deleted_jails_contribution() {
        let dir = test_state_dir("committed_resources_drops_a_deleted_jails_contribution");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply(sample_spec_with_resources("web-1", "2", "512M")).unwrap();
        reconciler.apply(sample_spec_with_resources("web-2", "1", "256M")).unwrap();
        reconciler.delete("web-1").unwrap();
        let (cpu, memory) = reconciler.committed_resources();
        assert_eq!(cpu, 1.0);
        assert_eq!(memory, 256 * 1024 * 1024);
    }

    #[test]
    fn add_alias_then_remove_alias_round_trips_through_the_fake_net_manager() {
        let dir = test_state_dir("add_alias_then_remove_alias_round_trips_through_the_fake_net_manager");
        let reconciler = new_reconciler(dir);
        reconciler.net.ensure_bridge_exists("keel0").unwrap();
        reconciler.add_alias("keel0", "10.0.250.7").unwrap();
        assert!(reconciler.net.has_alias("keel0", "10.0.250.7"));
        reconciler.remove_alias("keel0", "10.0.250.7").unwrap();
        assert!(!reconciler.net.has_alias("keel0", "10.0.250.7"));
    }

    #[test]
    fn apply_persists_and_tracks_the_record() {
        let dir = test_state_dir("apply_persists_and_tracks_the_record");
        let mut reconciler = new_reconciler(dir.clone());
        reconciler.apply(sample_spec("web-1")).unwrap();

        assert_eq!(reconciler.records.len(), 1);
        assert_eq!(reconciler.records["web-1"].epair_ordinal, 1);

        let loaded = store::load_all(&dir).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].spec.metadata.name, "web-1");
    }

    #[test]
    fn apply_assigns_increasing_ordinals_to_new_jails() {
        let dir = test_state_dir("apply_assigns_increasing_ordinals_to_new_jails");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply(sample_spec("web-1")).unwrap();
        reconciler.apply(sample_spec("web-2")).unwrap();
        assert_eq!(reconciler.records["web-1"].epair_ordinal, 1);
        assert_eq!(reconciler.records["web-2"].epair_ordinal, 2);
    }

    #[test]
    fn new_recovers_next_ordinal_from_disk() {
        let dir = test_state_dir("new_recovers_next_ordinal_from_disk");
        {
            let mut reconciler = new_reconciler(dir.clone());
            reconciler.apply(sample_spec("web-1")).unwrap();
            reconciler.apply(sample_spec("web-2")).unwrap();
        }
        // A fresh Reconciler over the same state_dir should pick up where the last one left off.
        let reconciler = new_reconciler(dir);
        assert_eq!(reconciler.next_epair_ordinal, 3);
    }

    #[test]
    fn apply_keeps_the_same_ordinal_on_reapply() {
        let dir = test_state_dir("apply_keeps_the_same_ordinal_on_reapply");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply(sample_spec("web-1")).unwrap();
        let first_ordinal = reconciler.records["web-1"].epair_ordinal;

        let mut updated = sample_spec("web-1");
        updated.spec.resources.cpu = "4".to_string(); // mutable field, allowed
        reconciler.apply(updated).unwrap();

        assert_eq!(reconciler.records["web-1"].epair_ordinal, first_ordinal);
    }

    #[test]
    fn apply_rejects_immutable_field_change() {
        let dir = test_state_dir("apply_rejects_immutable_field_change");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply(sample_spec("web-1")).unwrap();

        let mut changed = sample_spec("web-1");
        changed.spec.image = "base/different-image".to_string();
        let result = reconciler.apply(changed);
        assert!(matches!(result, Err(ReconcileError::InvalidSpec(_))));
    }

    #[test]
    fn apply_rejects_invalid_name() {
        let dir = test_state_dir("apply_rejects_invalid_name");
        let mut reconciler = new_reconciler(dir);
        let mut invalid = sample_spec("Invalid_Name");
        invalid.metadata.name = "Invalid_Name".to_string();
        let result = reconciler.apply(invalid);
        assert!(matches!(result, Err(ReconcileError::InvalidSpec(_))));
    }

    #[test]
    fn apply_rejects_an_image_outside_the_base_namespace() {
        let dir = test_state_dir("apply_rejects_an_image_outside_the_base_namespace");
        let mut reconciler = new_reconciler(dir);
        let mut spec = sample_spec("web-1");
        spec.spec.image = "jails/some-other-jail".to_string();
        let result = reconciler.apply(spec);
        assert!(matches!(result, Err(ReconcileError::InvalidSpec(_))));
    }

    #[test]
    fn apply_rejects_a_bridge_outside_the_keel_naming_convention() {
        let dir = test_state_dir("apply_rejects_a_bridge_outside_the_keel_naming_convention");
        let mut reconciler = new_reconciler(dir);
        let mut spec = sample_spec("web-1");
        spec.spec.network.bridge = "lo0".to_string();
        let result = reconciler.apply(spec);
        assert!(matches!(result, Err(ReconcileError::InvalidSpec(_))));
    }

    #[test]
    fn apply_rejects_a_volume_mount_path_with_a_traversal_segment() {
        let dir = test_state_dir("apply_rejects_a_volume_mount_path_with_a_traversal_segment");
        let mut reconciler = new_reconciler(dir);
        let spec = sample_spec_with_volume("web-1", "web-data", "../../../etc", "1G");
        let result = reconciler.apply(spec);
        assert!(matches!(result, Err(ReconcileError::InvalidSpec(_))));
    }

    #[test]
    fn delete_on_unknown_name_returns_not_found() {
        let dir = test_state_dir("delete_on_unknown_name_returns_not_found");
        let mut reconciler = new_reconciler(dir);
        assert!(matches!(reconciler.delete("missing"), Err(ReconcileError::NotFound(_))));
    }

    #[test]
    fn delete_removes_the_record_from_memory_and_disk() {
        let dir = test_state_dir("delete_removes_the_record_from_memory_and_disk");
        let mut reconciler = new_reconciler(dir.clone());
        reconciler.apply(sample_spec("web-1")).unwrap();

        reconciler.delete("web-1").unwrap();

        assert!(!reconciler.records.contains_key("web-1"));
        assert_eq!(store::load_all(&dir).unwrap(), vec![]);
    }

    #[test]
    fn delete_persists_a_deleting_marker_before_destructive_actions_and_can_be_retried_after_a_transient_failure() {
        let dir = test_state_dir("delete_persists_a_deleting_marker_before_destructive_actions");
        let mut reconciler = new_reconciler(dir.clone());
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec("web-1")).unwrap();
        reconciler.provision("web-1", &reconciler.records["web-1"].clone()).unwrap();

        // Force the rootfs dataset destroy to fail partway through delete()
        // (a real "device busy" is exactly the scenario destroy_dataset's
        // own retry loop exists for), simulating any transient failure
        // between the jail actually being destroyed and the record being
        // fully removed.
        let jail_dataset = record::jail_dataset_path("zroot", "web-1");
        reconciler.zfs.mark_busy(&jail_dataset);

        let result = reconciler.delete("web-1");
        assert!(matches!(result, Err(ReconcileError::Zfs(keel_zfs::ZfsError::Busy(_)))));

        // The record must survive this failure (on disk and in memory),
        // marked deleting -- not silently vanish (which would let a later
        // reconcile reprovision it) and not stay looking like a normal,
        // undeleted record either.
        assert!(reconciler.records["web-1"].deleting, "record must be marked deleting after a failed delete");
        let loaded = store::load_all(&dir).unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded[0].deleting);
        // The jail itself was already destroyed before the dataset step
        // failed, proving destructive actions genuinely ran.
        assert!(!reconciler.jails.jail_exists(&record::jail_name("web-1")).unwrap());

        // Retrying once the transient failure clears must finish the job.
        reconciler.zfs.unmark_busy(&jail_dataset);
        reconciler.delete("web-1").unwrap();
        assert!(!reconciler.records.contains_key("web-1"));
        assert_eq!(store::load_all(&dir).unwrap(), vec![]);
    }

    #[test]
    fn reconcile_resumes_a_pending_delete_after_a_simulated_restart_instead_of_reprovisioning() {
        // Proves the fix for "a crash between destroying a jail and
        // removing its record resurrects it": simulate that exact crash by
        // hand-persisting a `deleting: true` record for a jail that's still
        // actually alive in the fakes, then build a *fresh* `Reconciler::new`
        // over the same state_dir and same underlying fakes (mirroring
        // `resume_replication_loops_starts_a_loop_for_a_record_persisted_before_a_restart`'s
        // restart simulation) and confirm `reconcile()` finishes destroying
        // it rather than treating "jail not tracked as running yet" as
        // "provision me."
        let dir = test_state_dir("reconcile_resumes_a_pending_delete_after_a_simulated_restart");
        let mut reconciler = new_reconciler(dir.clone());
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec("web-1")).unwrap();
        reconciler.provision("web-1", &reconciler.records["web-1"].clone()).unwrap();
        assert!(reconciler.jails.jail_exists(&record::jail_name("web-1")).unwrap());

        // "The crash": the delete marker was persisted, but nothing else
        // ran yet -- the jail is still alive in the fakes' shared state.
        let mut mid_delete = reconciler.records["web-1"].clone();
        mid_delete.deleting = true;
        store::save(&dir, &mid_delete).unwrap();

        // "The restart": a fresh Reconciler over the same state_dir and the
        // same underlying fakes (jails/zfs/net/mounts all persist real
        // state across this "restart," a real daemon restart too).
        let mut restarted = Reconciler::new(
            reconciler.jails.clone(),
            reconciler.zfs.clone(),
            reconciler.net.clone(),
            reconciler.mounts.clone(),
            "zroot".to_string(),
            dir.clone(),
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        assert!(restarted.records["web-1"].deleting, "the loaded record must still be marked deleting");

        let failures = restarted.reconcile(Instant::now());
        assert!(failures.is_empty(), "expected the resumed delete to succeed cleanly: {failures:?}");

        assert!(
            !restarted.jails.jail_exists(&record::jail_name("web-1")).unwrap(),
            "the jail must have been destroyed, not reprovisioned"
        );
        assert!(!restarted.records.contains_key("web-1"));
        assert_eq!(store::load_all(&dir).unwrap(), vec![]);
    }

    #[test]
    fn provision_drives_zfs_jail_net_and_command_in_order() {
        let dir = test_state_dir("provision_drives_zfs_jail_net_and_command_in_order");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec("web-1")).unwrap();
        let record = reconciler.records["web-1"].clone();

        reconciler.provision("web-1", &record).unwrap();

        let jail_name = record::jail_name("web-1");
        assert_eq!(reconciler.jails.jail_exists(&jail_name).unwrap(), true);
        assert_eq!(reconciler.jails.is_running(&jail_name).unwrap(), true);
        assert_eq!(
            reconciler.zfs.dataset_exists(&record::jail_dataset_path("zroot", "web-1")).unwrap(),
            true
        );
    }

    #[test]
    fn provision_fails_clearly_when_base_image_missing() {
        let dir = test_state_dir("provision_fails_clearly_when_base_image_missing");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply(sample_spec("web-1")).unwrap();
        let record = reconciler.records["web-1"].clone();

        let result = reconciler.provision("web-1", &record);
        assert!(matches!(result, Err(ReconcileError::BaseImageNotFound(_))));
    }

    #[test]
    fn provision_creates_and_mounts_a_declared_volume() {
        let dir = test_state_dir("provision_creates_and_mounts_a_declared_volume");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec_with_volume("web-1", "web-data", "/data", "1G")).unwrap();
        let record = reconciler.records["web-1"].clone();

        reconciler.provision("web-1", &record).unwrap();

        assert!(reconciler.zfs.dataset_exists("zroot/keel/volumes/web-data").unwrap());
        let target = record::jail_rootfs_path("zroot", "web-1").join("data");
        assert!(reconciler.mounts.is_mounted(&target).unwrap());
    }

    #[test]
    fn restart_ensures_a_declared_volume_is_mounted_even_if_the_jail_was_created_without_it() {
        // Simulates a crash between `jails.create` succeeding and
        // `provision`'s volume-mount loop ever running: on restart,
        // `jail_exists` is true (the jail metadata survived) so
        // `reconcile_one` dispatches to `restart`, not `provision` -- which
        // must still ensure the declared volume before starting the jail's
        // command against what would otherwise be an empty directory.
        let dir = test_state_dir("restart_ensures_a_declared_volume_is_mounted_even_if_the_jail_was_created_without_it");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec_with_volume("web-1", "web-data", "/data", "1G")).unwrap();
        let record = reconciler.records["web-1"].clone();

        let rootfs = record::jail_rootfs_path("zroot", "web-1");
        reconciler.jails.create(&record::jail_name("web-1"), &rootfs, true).unwrap();
        let target = rootfs.join("data");
        assert!(!reconciler.mounts.is_mounted(&target).unwrap(), "the volume must not be mounted yet in this scenario");

        reconciler.restart("web-1", &record).unwrap();

        assert!(reconciler.zfs.dataset_exists("zroot/keel/volumes/web-data").unwrap());
        assert!(reconciler.mounts.is_mounted(&target).unwrap(), "restart must still ensure the declared volume is mounted");
    }

    #[test]
    fn reprovisioning_after_a_restart_does_not_recreate_or_remount_the_volume() {
        let dir = test_state_dir("reprovisioning_after_a_restart_does_not_recreate_or_remount_the_volume");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec_with_volume("web-1", "web-data", "/data", "1G")).unwrap();
        let record = reconciler.records["web-1"].clone();
        reconciler.provision("web-1", &record).unwrap();

        // Simulate an agentd restart with the jail already provisioned:
        // re-running provision must be a no-op for the volume (still
        // exactly one dataset, still mounted, no error).
        reconciler.jails.destroy(&record::jail_name("web-1")).unwrap();
        reconciler.provision("web-1", &record).unwrap();

        assert!(reconciler.zfs.dataset_exists("zroot/keel/volumes/web-data").unwrap());
        let target = record::jail_rootfs_path("zroot", "web-1").join("data");
        assert!(reconciler.mounts.is_mounted(&target).unwrap());
    }

    #[test]
    fn delete_unmounts_the_volume_but_leaves_its_dataset_present() {
        let dir = test_state_dir("delete_unmounts_the_volume_but_leaves_its_dataset_present");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec_with_volume("web-1", "web-data", "/data", "1G")).unwrap();
        let record = reconciler.records["web-1"].clone();
        reconciler.provision("web-1", &record).unwrap();

        reconciler.delete("web-1").unwrap();

        assert!(reconciler.zfs.dataset_exists("zroot/keel/volumes/web-data").unwrap(), "volume dataset must survive jail deletion");
        let target = record::jail_rootfs_path("zroot", "web-1").join("data");
        assert!(!reconciler.mounts.is_mounted(&target).unwrap());
    }

    #[test]
    fn reapplying_and_reprovisioning_the_same_name_finds_the_dataset_and_remounts_it() {
        let dir = test_state_dir("reapplying_and_reprovisioning_the_same_name_finds_the_dataset_and_remounts_it");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec_with_volume("web-1", "web-data", "/data", "1G")).unwrap();
        let record = reconciler.records["web-1"].clone();
        reconciler.provision("web-1", &record).unwrap();
        reconciler.delete("web-1").unwrap();

        reconciler.apply(sample_spec_with_volume("web-1", "web-data", "/data", "1G")).unwrap();
        let record = reconciler.records["web-1"].clone();
        reconciler.provision("web-1", &record).unwrap();

        assert!(reconciler.zfs.dataset_exists("zroot/keel/volumes/web-data").unwrap());
        let target = record::jail_rootfs_path("zroot", "web-1").join("data");
        assert!(reconciler.mounts.is_mounted(&target).unwrap());
    }

    #[test]
    fn get_volume_reports_existence() {
        let dir = test_state_dir("get_volume_reports_existence");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec_with_volume("web-1", "web-data", "/data", "1G")).unwrap();
        let record = reconciler.records["web-1"].clone();
        reconciler.provision("web-1", &record).unwrap();

        assert!(reconciler.get_volume("web-data").is_ok());
        assert!(matches!(reconciler.get_volume("missing"), Err(ReconcileError::Zfs(keel_zfs::ZfsError::NotFound(_)))));
    }

    #[test]
    fn delete_volume_destroys_the_dataset_once_the_jail_is_gone() {
        let dir = test_state_dir("delete_volume_destroys_the_dataset_once_the_jail_is_gone");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec_with_volume("web-1", "web-data", "/data", "1G")).unwrap();
        let record = reconciler.records["web-1"].clone();
        reconciler.provision("web-1", &record).unwrap();
        reconciler.delete("web-1").unwrap();

        reconciler.delete_volume("web-data").unwrap();

        assert!(matches!(reconciler.get_volume("web-data"), Err(ReconcileError::Zfs(keel_zfs::ZfsError::NotFound(_)))));
    }

    #[test]
    fn delete_volume_on_a_never_created_name_is_not_found() {
        let dir = test_state_dir("delete_volume_on_a_never_created_name_is_not_found");
        let mut reconciler = new_reconciler(dir);
        assert!(matches!(reconciler.delete_volume("missing"), Err(ReconcileError::Zfs(keel_zfs::ZfsError::NotFound(_)))));
    }

    #[test]
    fn list_volumes_returns_every_volume_dataset_sorted_by_name() {
        let zfs = FakeZfsManager::new();
        zfs.seed_dataset("zroot/keel/volumes/web-data");
        zfs.seed_dataset("zroot/keel/volumes/db-data");
        zfs.seed_dataset("zroot/keel/jails/web-1");
        let reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            zfs,
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            test_state_dir("list_volumes_returns_every_volume_dataset_sorted_by_name"),
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        let volumes = reconciler.list_volumes().unwrap();
        assert_eq!(volumes, vec![
            crate::wire::VolumeStatus { name: "db-data".to_string() },
            crate::wire::VolumeStatus { name: "web-data".to_string() },
        ]);
    }

    #[test]
    fn list_volumes_on_a_pool_with_no_volumes_is_empty() {
        let reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            FakeZfsManager::new(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            "zroot".to_string(),
            test_state_dir("list_volumes_on_a_pool_with_no_volumes_is_empty"),
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        assert_eq!(reconciler.list_volumes().unwrap(), vec![]);
    }

    #[test]
    fn rollback_provision_unmounts_a_declared_volume() {
        // A provision failure that happens after the volume-mount step (a
        // real `configure_networking_and_limits`/`start_command` failure,
        // not reproducible against these in-memory fakes) must not leave
        // the volume's nullfs mount sitting on top of the rootfs dataset:
        // an un-unmounted volume is exactly the "device busy" class of
        // failure `delete()`'s own unmount-before-destroy ordering already
        // exists to avoid, and leaving it in place would make every
        // subsequent retry's `zfs clone` fail with "already exists"
        // forever, wedging the reconciler's own backoff-retry loop. Since
        // the fakes can't force that failure directly, this instead
        // reproduces rollback_provision's required cleanup contract the
        // same way the test below does: run a fully successful provision
        // (which mounts the volume for real, against the fakes), then call
        // rollback_provision directly, exactly the state it must also
        // handle after a genuine partial failure.
        let dir = test_state_dir("rollback_provision_unmounts_a_declared_volume");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec_with_volume("web-1", "web-data", "/data", "1G")).unwrap();
        let record = reconciler.records["web-1"].clone();

        reconciler.provision("web-1", &record).unwrap();
        reconciler.rollback_provision("web-1", &record);

        let target = record::jail_rootfs_path("zroot", "web-1").join("data");
        assert_eq!(reconciler.mounts.is_mounted(&target).unwrap(), false, "volume mount must be undone by rollback");
    }

    #[test]
    fn rollback_provision_cleans_up_after_partial_failure() {
        let dir = test_state_dir("rollback_provision_cleans_up_after_partial_failure");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec("web-1")).unwrap();
        let record = reconciler.records["web-1"].clone();

        // Simulate a partial failure: dataset clone + jail create succeed,
        // then provisioning would fail at the networking step in the real
        // system. We can't easily force FakeNetManager to fail without a
        // missing bridge, so instead verify rollback's own idempotent
        // cleanup behavior directly: calling it after a successful
        // provision should fully undo it.
        reconciler.provision("web-1", &record).unwrap();
        reconciler.rollback_provision("web-1", &record);

        let jail_name = record::jail_name("web-1");
        assert_eq!(reconciler.jails.jail_exists(&jail_name).unwrap(), false);
        assert_eq!(
            reconciler.zfs.dataset_exists(&record::jail_dataset_path("zroot", "web-1")).unwrap(),
            false
        );
    }

    #[test]
    fn reconcile_provisions_a_missing_jail() {
        let dir = test_state_dir("reconcile_provisions_a_missing_jail");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec("web-1")).unwrap();

        let failures = reconciler.reconcile(Instant::now());

        assert!(failures.is_empty(), "expected no failures, got: {failures:?}");
        let jail_name = record::jail_name("web-1");
        assert_eq!(reconciler.jails.is_running(&jail_name).unwrap(), true);
    }

    #[test]
    fn reconcile_reports_base_image_not_found_without_stopping_other_jails() {
        let dir = test_state_dir("reconcile_reports_base_image_not_found_without_stopping_other_jails");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec("web-1")).unwrap(); // has a seeded base image
        let mut broken = sample_spec("web-2");
        broken.spec.image = "base/does-not-exist".to_string();
        reconciler.apply(broken).unwrap(); // base image never seeded

        let failures = reconciler.reconcile(Instant::now());

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].0, "web-2");
        assert!(matches!(failures[0].1, ReconcileError::BaseImageNotFound(_)));
        // web-1 should still have been provisioned successfully.
        assert_eq!(reconciler.jails.is_running(&record::jail_name("web-1")).unwrap(), true);
    }

    #[test]
    fn reconcile_restarts_a_crashed_jail() {
        let dir = test_state_dir("reconcile_restarts_a_crashed_jail");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec("web-1")).unwrap();
        let t0 = Instant::now();
        reconciler.reconcile(t0);

        let jail_name = record::jail_name("web-1");
        reconciler.jails.mark_exited(&jail_name);
        assert_eq!(reconciler.jails.is_running(&jail_name).unwrap(), false);

        // The initial provisioning call above already armed a 1s backoff
        // cooldown (record_attempt runs unconditionally after provisioning,
        // per BackoffState's contract) — advance past it so this restart
        // attempt isn't itself suppressed by that cooldown.
        reconciler.reconcile(t0 + Duration::from_secs(1));
        assert_eq!(reconciler.jails.is_running(&jail_name).unwrap(), true);
    }

    #[test]
    fn reconcile_respects_backoff_cooldown_between_restarts() {
        let dir = test_state_dir("reconcile_respects_backoff_cooldown_between_restarts");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec("web-1")).unwrap();
        let t0 = Instant::now();
        reconciler.reconcile(t0);

        let jail_name = record::jail_name("web-1");
        reconciler.jails.mark_exited(&jail_name);
        // Still within the 1s cooldown armed by the initial provisioning
        // call above — this reconcile is a no-op, same as the next one.
        reconciler.reconcile(t0);

        reconciler.jails.mark_exited(&jail_name);
        reconciler.reconcile(t0); // still within cooldown — should NOT restart yet
        assert_eq!(reconciler.jails.is_running(&jail_name).unwrap(), false);

        reconciler.reconcile(t0 + Duration::from_secs(1)); // cooldown passed
        assert_eq!(reconciler.jails.is_running(&jail_name).unwrap(), true);
    }

    #[test]
    fn reconcile_never_policy_leaves_a_crashed_jail_alone() {
        let dir = test_state_dir("reconcile_never_policy_leaves_a_crashed_jail_alone");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        let mut spec = sample_spec("web-1");
        spec.spec.restart_policy = RestartPolicy::Never;
        reconciler.apply(spec).unwrap();
        reconciler.reconcile(Instant::now());

        let jail_name = record::jail_name("web-1");
        reconciler.jails.mark_exited(&jail_name);
        reconciler.reconcile(Instant::now());

        assert_eq!(reconciler.jails.is_running(&jail_name).unwrap(), false);
    }

    #[test]
    fn reconcile_arms_backoff_even_when_restart_attempt_fails() {
        let dir = test_state_dir("reconcile_arms_backoff_even_when_restart_attempt_fails");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec("web-1")).unwrap();
        let t0 = Instant::now();
        reconciler.reconcile(t0); // provisions web-1

        let jail_name = record::jail_name("web-1");
        reconciler.jails.mark_exited(&jail_name);
        reconciler.jails.fail_start_command(&jail_name, true);

        // Past the 1s cooldown armed by the initial provisioning.
        let failures = reconciler.reconcile(t0 + Duration::from_secs(1));
        assert_eq!(failures.len(), 1, "expected the failed restart attempt to be reported: {failures:?}");

        // If backoff was correctly armed by the failed restart attempt,
        // an immediate retry at the same instant must be suppressed —
        // proven by the fault (still armed) NOT firing again.
        let retried = reconciler.reconcile(t0 + Duration::from_secs(1));
        assert!(retried.is_empty(), "expected the retry to be suppressed by backoff, got: {retried:?}");
        assert_eq!(reconciler.jails.is_running(&jail_name).unwrap(), false);

        // Once the cooldown clears and the fault is removed, it should recover.
        reconciler.jails.fail_start_command(&jail_name, false);
        let recovered = reconciler.reconcile(t0 + Duration::from_secs(10));
        assert!(recovered.is_empty(), "expected recovery, got: {recovered:?}");
        assert_eq!(reconciler.jails.is_running(&jail_name).unwrap(), true);
    }

    #[test]
    fn reconcile_is_a_no_op_when_jail_already_matches_desired_state() {
        let dir = test_state_dir("reconcile_is_a_no_op_when_jail_already_matches_desired_state");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec("web-1")).unwrap();
        reconciler.reconcile(Instant::now());

        let failures = reconciler.reconcile(Instant::now());
        assert!(failures.is_empty(), "expected no failures, got: {failures:?}");
        assert_eq!(reconciler.jails.is_running(&record::jail_name("web-1")).unwrap(), true);
    }

    #[test]
    fn get_returns_none_for_an_unknown_name() {
        let dir = test_state_dir("get_returns_none_for_an_unknown_name");
        let reconciler = new_reconciler(dir);
        assert!(reconciler.get("missing", Instant::now()).is_none());
    }

    #[test]
    fn get_reports_spec_running_state_and_backoff_after_provisioning() {
        let dir = test_state_dir("get_reports_spec_running_state_and_backoff_after_provisioning");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec("web-1")).unwrap();
        let t0 = Instant::now();
        reconciler.reconcile(t0);

        let status = reconciler.get("web-1", t0).unwrap();
        assert_eq!(status.record.spec.metadata.name, "web-1");
        assert!(status.running);
        assert_eq!(status.backoff.retry_in_secs, Some(1));
    }

    #[test]
    fn list_returns_all_records_sorted_by_name() {
        let dir = test_state_dir("list_returns_all_records_sorted_by_name");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply(sample_spec("web-2")).unwrap();
        reconciler.apply(sample_spec("web-1")).unwrap();

        let statuses = reconciler.list(Instant::now());
        let names: Vec<&str> = statuses.iter().map(|s| s.record.spec.metadata.name.as_str()).collect();
        assert_eq!(names, vec!["web-1", "web-2"]);
    }

    #[test]
    fn list_is_empty_when_no_specs_have_been_applied() {
        let dir = test_state_dir("list_is_empty_when_no_specs_have_been_applied");
        let reconciler = new_reconciler(dir);
        assert!(reconciler.list(Instant::now()).is_empty());
    }

    #[test]
    fn set_replicate_to_updates_the_record_without_going_through_validate_transition() {
        let dir = test_state_dir("set_replicate_to_updates_the_record_without_going_through_validate_transition");
        let mut reconciler = new_reconciler(dir.clone());
        reconciler.zfs.seed_dataset("zroot/keel/base/14.2-web");
        reconciler.apply(sample_spec_with_volume("db-0", "db-0-data", "/var/db", "5G")).unwrap();

        reconciler.set_replicate_to("db-0", Some("10.0.0.9:7622".to_string())).unwrap();

        assert_eq!(reconciler.records["db-0"].spec.spec.replicate_to, Some("10.0.0.9:7622".to_string()));
        let loaded = store::load_all(&dir).unwrap();
        assert_eq!(loaded[0].spec.spec.replicate_to, Some("10.0.0.9:7622".to_string()));
    }

    #[test]
    fn set_replicate_to_on_an_unknown_name_returns_not_found() {
        let dir = test_state_dir("set_replicate_to_on_an_unknown_name_returns_not_found");
        let mut reconciler = new_reconciler(dir);
        assert!(matches!(reconciler.set_replicate_to("missing", Some("10.0.0.9:7622".to_string())), Err(ReconcileError::NotFound(_))));
    }

    fn sample_ingress_spec(name: &str, host: &str) -> keel_spec::IngressSpec {
        keel_spec::IngressSpec {
            api_version: "keel/v1".to_string(),
            kind: "Ingress".to_string(),
            metadata: keel_spec::Metadata { name: name.to_string() },
            spec: keel_spec::IngressSpecBody {
                host: host.to_string(),
                backend: keel_spec::IngressBackend { service: "hugo-site".to_string(), port: 8080 },
                tls: keel_spec::IngressTls { email: "admin@example.com".to_string() },
            },
        }
    }

    #[test]
    fn apply_ingress_persists_and_tracks_the_record() {
        let dir = test_state_dir("apply_ingress_persists_and_tracks_the_record");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        assert_eq!(reconciler.list_ingress().len(), 1);
        assert_eq!(reconciler.get_ingress("blog").unwrap().spec.spec.host, "example.com");
    }

    #[test]
    fn apply_ingress_survives_a_simulated_restart() {
        let dir = test_state_dir("apply_ingress_survives_a_simulated_restart");
        {
            let mut reconciler = new_reconciler(dir.clone());
            reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        }
        let reloaded = new_reconciler(dir);
        assert_eq!(reloaded.list_ingress().len(), 1);
    }

    #[test]
    fn get_ingress_on_an_unknown_name_returns_none() {
        let dir = test_state_dir("get_ingress_on_an_unknown_name_returns_none");
        let reconciler = new_reconciler(dir);
        assert_eq!(reconciler.get_ingress("missing"), None);
    }

    #[test]
    fn delete_ingress_removes_the_record() {
        let dir = test_state_dir("delete_ingress_removes_the_record");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        reconciler.delete_ingress("blog").unwrap();
        assert_eq!(reconciler.list_ingress().len(), 0);
    }

    #[test]
    fn delete_ingress_on_an_unknown_name_is_not_found() {
        let dir = test_state_dir("delete_ingress_on_an_unknown_name_is_not_found");
        let mut reconciler = new_reconciler(dir);
        assert!(matches!(reconciler.delete_ingress("missing"), Err(ReconcileError::NotFound(_))));
    }

    #[test]
    fn re_applying_an_existing_ingress_updates_its_host_and_keeps_the_same_name() {
        let dir = test_state_dir("re_applying_an_existing_ingress_updates_its_host");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        reconciler.apply_ingress(sample_ingress_spec("blog", "blog.example.com")).unwrap();
        assert_eq!(reconciler.list_ingress().len(), 1);
        assert_eq!(reconciler.get_ingress("blog").unwrap().spec.spec.host, "blog.example.com");
    }

    #[test]
    fn re_applying_an_existing_ingress_preserves_cert_expires_at_unix() {
        let dir = test_state_dir("re_applying_an_existing_ingress_preserves_cert_expires_at_unix");
        let mut reconciler = new_reconciler(dir);

        // Apply ingress with initial host
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();

        // Manually set cert_expires_at_unix to simulate a certificate that was issued
        let cert_timestamp = 1_800_000_000i64;
        reconciler.ingress_records.get_mut("blog").unwrap().cert_expires_at_unix = Some(cert_timestamp);

        // Re-apply the same ingress with a different host
        reconciler.apply_ingress(sample_ingress_spec("blog", "blog.example.com")).unwrap();

        // Verify the certificate expiry is preserved despite the re-apply
        let record = reconciler.get_ingress("blog").unwrap();
        assert_eq!(record.cert_expires_at_unix, Some(cert_timestamp), "cert_expires_at_unix must be preserved across re-apply");
        assert_eq!(record.spec.spec.host, "blog.example.com", "host should be updated to the new value");
    }

    #[test]
    fn reconcile_provisions_the_singleton_ingress_jail_once_an_ingress_spec_exists() {
        let dir = test_state_dir("reconcile_provisions_the_singleton_ingress_jail");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/keel-ingress");
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        reconciler.reconcile(Instant::now());
        assert!(reconciler.jails.jail_exists("keel-ingress").unwrap());
    }

    #[test]
    fn reconcile_does_not_provision_the_ingress_jail_when_no_ingress_spec_exists() {
        let dir = test_state_dir("reconcile_does_not_provision_the_ingress_jail_when_none_exist");
        let mut reconciler = new_reconciler(dir);
        reconciler.reconcile(Instant::now());
        assert!(!reconciler.jails.jail_exists("keel-ingress").unwrap());
    }

    #[test]
    fn reconcile_provisions_only_one_ingress_jail_even_as_more_ingress_specs_are_applied() {
        let dir = test_state_dir("reconcile_provisions_only_one_ingress_jail");
        let record_path = dir.join("ingress.yaml");
        let mut reconciler = new_reconciler(dir);
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        reconciler.reconcile(Instant::now());
        let ordinal_after_first = reconciler.records.get("ingress").unwrap().epair_ordinal;
        assert_eq!(reconciler.records.keys().filter(|name| name.as_str() == "ingress").count(), 1);

        // `apply()` is itself idempotent for a pre-existing record name (it
        // reuses the existing epair_ordinal and writes a byte-identical
        // on-disk record), so an *unguarded* `ensure_ingress_jail` that
        // called `self.apply(spec)` on every single `reconcile()` tick,
        // completely bypassing the "already exists" check, would still
        // pass the two assertions above: an unchanged ordinal and exactly
        // one "ingress" key. Capture the on-disk record's mtime here so the
        // next `reconcile()` call below can prove the guard actually
        // suppresses the second `apply()` call (and thus the second
        // `store::save`), not merely that a redundant one would be
        // harmless.
        let mtime_after_first =
            fs::metadata(&record_path).unwrap().modified().unwrap();
        // Guard against filesystems with coarse mtime resolution: give the
        // clock room to tick forward before the next write, so that IF a
        // spurious rewrite happens, its mtime is observably different.
        std::thread::sleep(Duration::from_millis(1100));

        reconciler.apply_ingress(sample_ingress_spec("docs", "docs.example.com")).unwrap();
        reconciler.reconcile(Instant::now());
        assert_eq!(reconciler.records.keys().filter(|name| name.as_str() == "ingress").count(), 1);
        assert_eq!(
            reconciler.records.get("ingress").unwrap().epair_ordinal,
            ordinal_after_first,
            "the ingress jail must not be re-provisioned (and so not re-assigned a new epair ordinal) once it already exists"
        );
        let mtime_after_second =
            fs::metadata(&record_path).unwrap().modified().unwrap();
        assert_eq!(
            mtime_after_first, mtime_after_second,
            "ensure_ingress_jail must not rewrite ingress.yaml on the second reconcile() call; a changed mtime means \
             the `self.records.contains_key(\"ingress\")` guard was bypassed and apply() (and its store::save) ran again"
        );
    }

    #[test]
    fn deleting_the_last_ingress_spec_does_not_retroactively_destroy_the_ingress_jail() {
        let dir = test_state_dir("deleting_the_last_ingress_spec_does_not_destroy_the_jail");
        let mut reconciler = new_reconciler(dir);
        reconciler.zfs.seed_dataset("zroot/keel/base/keel-ingress");
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        reconciler.reconcile(Instant::now());
        reconciler.delete_ingress("blog").unwrap();
        reconciler.reconcile(Instant::now());
        assert!(reconciler.jails.jail_exists("keel-ingress").unwrap());
    }

    /// Wraps `FakeZfsManager`, recording (for the ingress jail's dataset
    /// specifically) whether a given real filesystem path already exists at
    /// the moment `clone_from_base` is called -- the only way to prove
    /// `reconcile()`'s ordering is correct, since the fake itself has no
    /// concept of a ZFS mountpoint colliding with a plain directory that was
    /// already written to (unlike a real `zfs clone`, which does).
    struct IngressCloneOrderSpyZfs {
        inner: FakeZfsManager,
        ingress_dataset: String,
        watched_path: PathBuf,
        path_existed_at_clone_time: std::sync::Arc<std::sync::Mutex<Option<bool>>>,
    }

    impl ZfsManager for IngressCloneOrderSpyZfs {
        fn dataset_exists(&self, dataset: &str) -> Result<bool, keel_zfs::ZfsError> {
            self.inner.dataset_exists(dataset)
        }
        fn clone_from_base(&self, base_dataset: &str, target_dataset: &str) -> Result<(), keel_zfs::ZfsError> {
            if target_dataset == self.ingress_dataset {
                *self.path_existed_at_clone_time.lock().unwrap() = Some(self.watched_path.exists());
            }
            self.inner.clone_from_base(base_dataset, target_dataset)
        }
        fn create_volume(&self, dataset: &str, quota: &str) -> Result<(), keel_zfs::ZfsError> {
            self.inner.create_volume(dataset, quota)
        }
        fn destroy_dataset(&self, dataset: &str) -> Result<(), keel_zfs::ZfsError> {
            self.inner.destroy_dataset(dataset)
        }
        fn snapshot(&self, dataset: &str, snapshot: &str) -> Result<(), keel_zfs::ZfsError> {
            self.inner.snapshot(dataset, snapshot)
        }
        fn destroy_snapshot(&self, dataset: &str, snapshot: &str) -> Result<(), keel_zfs::ZfsError> {
            self.inner.destroy_snapshot(dataset, snapshot)
        }
        fn send_snapshot(&self, dataset: &str, snapshot: &str, base: Option<&str>, out: &mut dyn std::io::Write) -> Result<(), keel_zfs::ZfsError> {
            self.inner.send_snapshot(dataset, snapshot, base, out)
        }
        fn receive_snapshot(&self, dataset: &str, input: &mut dyn std::io::Read) -> Result<(), keel_zfs::ZfsError> {
            self.inner.receive_snapshot(dataset, input)
        }
        fn list_child_datasets(&self, parent: &str) -> Result<Vec<String>, keel_zfs::ZfsError> {
            self.inner.list_child_datasets(parent)
        }
    }

    #[test]
    fn reconcile_provisions_the_ingress_jail_before_writing_its_certificate_into_it() {
        // Reproduces the ordering bug directly: `write_cert_to_ingress_jail`
        // used to run (via reconcile_certs) before the ingress jail's
        // dataset was ever cloned, so on a fresh node the first Ingress
        // apply wrote real cert files into a plain directory that a real
        // `zfs clone` then had to contend with as its own mountpoint.
        let dir = test_state_dir("reconcile_provisions_the_ingress_jail_before_writing_its_certificate_into_it");
        let pool = dir.strip_prefix("/").unwrap_or(&dir).to_string_lossy().into_owned();
        let ingress_dataset = record::jail_dataset_path(&pool, "ingress");
        let cert_path =
            record::jail_rootfs_path(&pool, "ingress").join("usr/local/etc/nginx/certs").join("example.com.crt");

        let inner = FakeZfsManager::new();
        inner.seed_dataset(&record::base_dataset_path(&pool, "base/keel-ingress"));
        let path_existed_at_clone_time = std::sync::Arc::new(std::sync::Mutex::new(None));
        let zfs = IngressCloneOrderSpyZfs {
            inner,
            ingress_dataset,
            watched_path: cert_path.clone(),
            path_existed_at_clone_time: path_existed_at_clone_time.clone(),
        };

        let mut reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            zfs,
            FakeNetManager::new(),
            FakeMountManager::new(),
            pool,
            dir,
            Box::new(keel_ingress::FakeAcmeClient::new()),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();

        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        reconciler.reconcile(Instant::now());

        assert_eq!(
            *path_existed_at_clone_time.lock().unwrap(),
            Some(false),
            "the cert file must not exist yet at the moment the ingress jail's dataset is cloned"
        );
        assert!(cert_path.is_file(), "the cert must still get written by the end of reconcile");
    }

    // `write_cert_to_ingress_jail` performs a genuine `std::fs::write` under
    // `record::jail_rootfs_path(&pool, "ingress")`. In production that path
    // is rooted in a real ZFS pool (e.g. "/zroot/..."), which a non-root
    // test process cannot create. Rather than mocking that write away (and
    // so never actually proving the certs land on disk), derive the "pool"
    // name for these tests from the test's own writable temp `dir`, so
    // `jail_rootfs_path` resolves to a real, writable location nested
    // inside it and the write genuinely happens end to end.
    fn test_reconciler_with_acme(
        dir: &std::path::Path,
        acme: keel_ingress::FakeAcmeClient,
        dns: keel_ingress::FakeDnsProvider,
    ) -> Reconciler<FakeJailRuntime, FakeZfsManager, FakeNetManager, FakeMountManager> {
        let pool = dir.strip_prefix("/").unwrap_or(dir).to_string_lossy().into_owned();
        Reconciler::new(
            FakeJailRuntime::new(),
            FakeZfsManager::new(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            pool,
            dir.to_path_buf(),
            Box::new(acme),
            Box::new(dns),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap()
    }

    fn certs_dir_for(reconciler: &Reconciler<FakeJailRuntime, FakeZfsManager, FakeNetManager, FakeMountManager>) -> PathBuf {
        record::jail_rootfs_path(&reconciler.pool, "ingress").join("usr/local/etc/nginx/certs")
    }

    #[test]
    fn reconcile_certs_issues_a_certificate_for_a_new_ingress_with_no_expiry_yet() {
        let dir = test_state_dir("reconcile_certs_issues_a_certificate_for_a_new_ingress");
        let mut reconciler = test_reconciler_with_acme(&dir, keel_ingress::FakeAcmeClient::new(), keel_ingress::FakeDnsProvider::new());
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();

        reconciler.reconcile_certs(1_800_000_000);

        assert!(reconciler.get_ingress("blog").unwrap().cert_expires_at_unix.is_some());
        let certs_dir = certs_dir_for(&reconciler);
        assert!(fs::read_to_string(certs_dir.join("example.com.crt")).unwrap().contains("example.com"));
        assert!(fs::read_to_string(certs_dir.join("example.com.key")).unwrap().contains("PRIVATE KEY"));
    }

    #[test]
    fn reconcile_certs_does_not_reissue_a_certificate_with_more_than_30_days_left() {
        let dir = test_state_dir("reconcile_certs_does_not_reissue_a_fresh_certificate");
        let mut reconciler = test_reconciler_with_acme(&dir, keel_ingress::FakeAcmeClient::new(), keel_ingress::FakeDnsProvider::new());
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        let now = 1_800_000_000;
        reconciler.reconcile_certs(now);
        let first_expiry = reconciler.get_ingress("blog").unwrap().cert_expires_at_unix;

        // Only 60 seconds later, well inside the 30-day threshold: the
        // expiry must be untouched by this second pass.
        reconciler.reconcile_certs(now + 60);
        assert_eq!(reconciler.get_ingress("blog").unwrap().cert_expires_at_unix, first_expiry);
    }

    #[test]
    fn reconcile_certs_reissues_when_the_cert_file_is_missing_even_with_a_fresh_expiry_timestamp() {
        // Reproduces a real bug found during Milestone 21 VM verification:
        // the ingress jail's rootfs was recreated (destroyed and
        // reprovisioned) after a cert had already been issued into it, and
        // `cert_expires_at_unix` (persisted separately from the jail's own
        // filesystem) still claimed the cert was valid and unexpired, so
        // `reconcile_certs` never re-issued a replacement even though the
        // actual `.crt`/`.key` files were gone from the new rootfs.
        let dir = test_state_dir("reconcile_certs_reissues_when_the_cert_file_is_missing");
        let mut reconciler = test_reconciler_with_acme(&dir, keel_ingress::FakeAcmeClient::new(), keel_ingress::FakeDnsProvider::new());
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        let now = 1_800_000_000;
        reconciler.reconcile_certs(now);
        assert!(reconciler.get_ingress("blog").unwrap().cert_expires_at_unix.is_some());

        // Simulate the jail's rootfs being lost/recreated: delete the cert
        // files on disk without touching the persisted `cert_expires_at_unix`.
        let certs_dir = certs_dir_for(&reconciler);
        fs::remove_file(certs_dir.join("example.com.crt")).unwrap();
        fs::remove_file(certs_dir.join("example.com.key")).unwrap();

        // Only 60 seconds later, well inside the 30-day threshold - but the
        // missing cert file must still force reissuance.
        reconciler.reconcile_certs(now + 60);
        assert!(fs::read_to_string(certs_dir.join("example.com.crt")).unwrap().contains("example.com"), "cert file should have been reissued");
    }

    #[test]
    fn reconcile_certs_reissues_within_30_days_of_expiry() {
        let dir = test_state_dir("reconcile_certs_reissues_within_30_days_of_expiry");
        let mut reconciler = test_reconciler_with_acme(&dir, keel_ingress::FakeAcmeClient::new(), keel_ingress::FakeDnsProvider::new());
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        let now = 1_800_000_000;
        reconciler.reconcile_certs(now);
        let first_expiry = reconciler.get_ingress("blog").unwrap().cert_expires_at_unix.unwrap();

        // Jump to 29 days before that expiry -- inside the 30-day threshold.
        let near_expiry = first_expiry - 29 * 24 * 60 * 60;
        reconciler.reconcile_certs(near_expiry);
        let second_expiry = reconciler.get_ingress("blog").unwrap().cert_expires_at_unix.unwrap();
        assert!(second_expiry > first_expiry, "renewal should push the expiry further into the future");
    }

    /// `FakeAcmeClient` has a single global `fail` flag, which would fail
    /// identically for every Ingress sharing this one `Reconciler`'s `acme`
    /// field. To prove that one Ingress's failing certificate issuance does
    /// not block another's within the same `reconcile_certs` pass, this
    /// double instead fails only for one specific domain.
    struct FlakyAcmeClient {
        fail_for_domain: String,
    }

    impl keel_ingress::AcmeClient for FlakyAcmeClient {
        fn request_certificate(
            &self,
            domain: &str,
            _contact_email: &str,
            _dns: &dyn keel_ingress::DnsProvider,
        ) -> Result<keel_ingress::Cert, keel_ingress::AcmeError> {
            if domain == self.fail_for_domain {
                return Err(keel_ingress::AcmeError::Request(format!("simulated ACME failure for '{domain}'")));
            }
            Ok(keel_ingress::Cert {
                cert_pem: format!("-----BEGIN CERTIFICATE-----\nFAKE CERT FOR {domain}\n-----END CERTIFICATE-----\n"),
                key_pem: "-----BEGIN PRIVATE KEY-----\nFAKE KEY\n-----END PRIVATE KEY-----\n".to_string(),
            })
        }
    }

    fn test_reconciler_with_acme_and_nginx(
        dir: &std::path::Path,
        acme: keel_ingress::FakeAcmeClient,
        dns: keel_ingress::FakeDnsProvider,
    ) -> (Reconciler<FakeJailRuntime, FakeZfsManager, FakeNetManager, FakeMountManager>, std::sync::Arc<crate::nginx::FakeNginxController>) {
        let mut reconciler = test_reconciler_with_acme(dir, acme, dns);
        let nginx = std::sync::Arc::new(crate::nginx::FakeNginxController::new());
        reconciler.nginx = Box::new(std::sync::Arc::clone(&nginx));
        reconciler.service_vips = crate::ServiceVipSlot::new();
        (reconciler, nginx)
    }

    #[test]
    fn reconcile_ingress_config_writes_and_reloads_nginx_once_a_backend_vip_is_known() {
        let dir = test_state_dir("reconcile_ingress_config_writes_and_reloads_nginx_once_a_backend_vip_is_known");
        let (mut reconciler, nginx) = test_reconciler_with_acme_and_nginx(&dir, keel_ingress::FakeAcmeClient::new(), keel_ingress::FakeDnsProvider::new());
        reconciler.service_vips.set_all(&[keel_controlplane::wire::ServiceProxyEntry {
            name: "hugo-site".to_string(),
            vip: "10.0.0.9".to_string(),
            port: 8080,
            replicas: vec![],
        }]);
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        reconciler.reconcile_ingress_config();
        let config = nginx.last_written_config("keel-ingress").unwrap();
        assert!(config.contains("server_name example.com;"));
        assert!(config.contains("proxy_pass http://10.0.0.9:8080;"));
        assert_eq!(nginx.reload_count("keel-ingress"), 1);
    }

    #[test]
    fn reconcile_ingress_config_skips_a_backend_whose_vip_is_not_yet_known() {
        let dir = test_state_dir("reconcile_ingress_config_skips_a_backend_whose_vip_is_not_yet_known");
        let (mut reconciler, nginx) = test_reconciler_with_acme_and_nginx(&dir, keel_ingress::FakeAcmeClient::new(), keel_ingress::FakeDnsProvider::new());
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        reconciler.reconcile_ingress_config();
        let config = nginx.last_written_config("keel-ingress").unwrap();
        assert!(!config.contains("example.com"));
    }

    #[test]
    fn reconcile_ingress_config_does_not_reload_when_validation_fails() {
        let dir = test_state_dir("reconcile_ingress_config_does_not_reload_when_validation_fails");
        let (mut reconciler, nginx) = test_reconciler_with_acme_and_nginx(&dir, keel_ingress::FakeAcmeClient::new(), keel_ingress::FakeDnsProvider::new());
        reconciler.service_vips.set_all(&[keel_controlplane::wire::ServiceProxyEntry {
            name: "hugo-site".to_string(),
            vip: "10.0.0.9".to_string(),
            port: 8080,
            replicas: vec![],
        }]);
        reconciler.apply_ingress(sample_ingress_spec("blog", "example.com")).unwrap();
        nginx.set_fail_test(true);
        reconciler.reconcile_ingress_config();
        assert_eq!(nginx.reload_count("keel-ingress"), 0);
    }

    #[test]
    fn reconcile_certs_backs_off_on_failure_without_blocking_other_ingresses() {
        let dir = test_state_dir("reconcile_certs_backs_off_on_failure_without_blocking_others");
        let pool = dir.strip_prefix("/").unwrap_or(&dir).to_string_lossy().into_owned();
        let acme = FlakyAcmeClient { fail_for_domain: "broken.example.com".to_string() };
        let mut reconciler = Reconciler::new(
            FakeJailRuntime::new(),
            FakeZfsManager::new(),
            FakeNetManager::new(),
            FakeMountManager::new(),
            pool,
            dir,
            Box::new(acme),
            Box::new(keel_ingress::FakeDnsProvider::new()),
            Box::new(crate::nginx::FakeNginxController::new()),
            crate::ServiceVipSlot::new(),
        )
        .unwrap();
        reconciler.apply_ingress(sample_ingress_spec("broken", "broken.example.com")).unwrap();
        reconciler.apply_ingress(sample_ingress_spec("healthy", "healthy.example.com")).unwrap();

        reconciler.reconcile_certs(1_800_000_000);

        assert_eq!(reconciler.get_ingress("broken").unwrap().cert_expires_at_unix, None);
        assert!(
            reconciler.get_ingress("healthy").unwrap().cert_expires_at_unix.is_some(),
            "one Ingress's failing ACME client must not block certificate issuance for another Ingress"
        );
    }
}
