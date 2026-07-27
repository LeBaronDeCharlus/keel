# lebaron.sh on the Milestone 21 VPS

Real production deployment: FreeBSD 15.0 VPS at `51.91.99.155`
(`ssh -p7522 root@51.91.99.155`), keel cluster of one node (`node-1`),
serving `https://lebaron.sh/` with a real Let's Encrypt certificate.

- Backend: `kind: Service` named `hugo-site-v2` (see
  `service-hugo-site-v2.yaml`), a `python3.11 -m http.server 8080
  --directory /var/www` jail (image `base/test`; there's no purpose-built
  Hugo image). Its jail is `hugo-site-v2-0`.
- Frontend: `kind: Ingress` named `blog` (see `ingress-blog.yaml`), host
  `lebaron.sh`, TLS via OVH DNS-01 + Let's Encrypt, proxied through the
  `ingress` nginx jail.
- The site content itself is just static files under
  `/zroot/keel/jails/hugo-site-v2-0/var/www` on the VPS, the backend
  jail doesn't know or care it's Hugo output, it just serves a directory.

## Redeploying the site content (new posts, edits, etc.)

The Hugo source lives at github.com/LeBaronDeCharlus/lebaron.sh, not in
this repo. Hugo isn't installed on the VPS, so build locally and push the
static output over:

```sh
git clone https://github.com/LeBaronDeCharlus/lebaron.sh.git /tmp/lebaron.sh
cd /tmp/lebaron.sh
hugo --minify        # -> public/

tar --no-xattrs --no-acls --no-mac-metadata -czf - -C public . \
  | ssh -p7522 root@51.91.99.155 '
      mkdir -p /zroot/keel/jails/hugo-site-v2-0/var/www.new &&
      tar xzf - -C /zroot/keel/jails/hugo-site-v2-0/var/www.new &&
      rm -rf /zroot/keel/jails/hugo-site-v2-0/var/www.old &&
      mv /zroot/keel/jails/hugo-site-v2-0/var/www /zroot/keel/jails/hugo-site-v2-0/var/www.old &&
      mv /zroot/keel/jails/hugo-site-v2-0/var/www.new /zroot/keel/jails/hugo-site-v2-0/var/www &&
      echo DEPLOY_OK'
```

The `--no-xattrs --no-acls --no-mac-metadata` flags matter: macOS's
`bsdtar` embeds a `com.apple.provenance` xattr that FreeBSD's `bsdtar`
can't restore, which aborts the extraction otherwise. `rsync` isn't
installed on the VPS, hence tar-over-ssh instead.

The `python3.11 -m http.server` process serves the directory live,
no restart needed after swapping `var/www`. The previous version sits at
`var/www.old` as a one-step rollback (`mv var/www.old var/www` back).

Verify:

```sh
curl -s -o /dev/null -w '%{http_code}\n' https://lebaron.sh/
```

## Applying/re-applying the Service and Ingress specs

Only needed when the specs themselves change (port, image, resources,
host, TLS email), **or** after any `keel-controlplane` restart, see
"Known gotcha" below. Both files in this directory are copy-pasted from
what's actually applied on the VPS (`/root/test-service.yaml` /
`/root/test-ingress.yaml`, and `/usr/local/etc/keel/services/hugo-site-v2.yaml`
for the auto-reseed copy).

Service (routes through the control plane, not a specific node):

```sh
ssh -p7522 root@51.91.99.155 \
  keelctl --control-plane-addr 127.0.0.1:7620 \
    --tls-ca-file /etc/keel/ca.crt \
    --tls-cert-file /etc/keel/admin.crt \
    --tls-key-file /etc/keel/admin.key \
    --tls-crl-file /etc/keel/crl.pem \
    apply -f - < service-hugo-site-v2.yaml
```

Ingress (always local to the node's agentd, never control-plane-routed):

```sh
ssh -p7522 root@51.91.99.155 \
  keelctl --socket /var/run/keel-agentd.sock apply -f - < ingress-blog.yaml
```

If you change `service-hugo-site-v2.yaml`, also update the copy at
`/usr/local/etc/keel/services/hugo-site-v2.yaml` on the VPS (that's the
one `keel_seed_services` re-applies on every boot, see below).

## The three `rc.d` services

Installed under `/usr/local/etc/rc.d/`, sources tracked in this repo at
`keel-controlplane/rc.d/`, `keel-agentd/rc.d/`, and `keelctl/rc.d/`.
Config lives in `/etc/rc.conf` on the VPS (~24 `keel_*` vars; a backup of
the pre-rc.d rc.conf is at `/etc/rc.conf.bak-before-keel-rcd`).

- `keel_controlplane`, `REQUIRE: NETWORKING`
- `keel_agentd`, `REQUIRE: NETWORKING keel_controlplane`
- `keel_seed_services`, `REQUIRE: keel_controlplane keel_agentd`, a
  one-shot (not a persistent daemon) that re-applies every `*.yaml` in
  `/usr/local/etc/keel/services/` via `keelctl`, retrying for ~20s in
  case the control plane isn't listening yet.

Standard commands apply: `service keel_agentd status`, `service
keel_controlplane restart`, etc. `keel_seed_services` has no meaningful
stop/status (it's a one-shot); re-running `service keel_seed_services
start` is always safe (idempotent `apply`).

## Fixed gotcha: `keel-controlplane` restart no longer forgets state

As of 2026-07-24, `keel-controlplane` persists `Services`/`Placements`/
`UsedAddresses`/`Standbys`/`PendingFences` to disk (default
`/var/db/keel-controlplane`, overridable via `--state-dir` /
`keel_controlplane_state_dir` in `/etc/rc.conf`) and reloads them at
startup, see
`docs/superpowers/plans/2026-07-24-keel-controlplane-persistence-and-fencing.md`.
A restart no longer forgets the `hugo-site-v2` Service or duplicates its
already-running jail; `keel_seed_services` remains in place as a
belt-and-suspenders reapply-on-boot mechanism (idempotent either way).
Verified live on this VPS on 2026-07-24: two real restarts with the new
binary, no duplicate jail, same placement/address before and after.

Where it can still bite (unrelated to persistence): if you ever see
`keel-controlplane` log
`failed to schedule replica 'hugo-site-v2-0' on node 'node-1': status
409, ... 'spec.network.address' cannot be changed`, it means the
scheduler's fresh address computation doesn't match the real running
jail's address (can happen if some other placement occupied a lower
address in the pod CIDR back when this jail was first created). Simply
re-applying the Service won't fix that. The fix is:

```sh
ssh -p7522 root@51.91.99.155 keelctl --socket /var/run/keel-agentd.sock delete hugo-site-v2-0
# wait ~10s for keel-controlplane's next reconcile tick to recreate it
# at whatever address it computes -- this time it'll match, since
# nothing else is contending for the low addresses
```

...then redeploy the site content (see above), since recreating the jail
re-clones its rootfs from the base image and wipes `/var/www`.

## Verifying after any change

```sh
curl -s -o /dev/null -w '%{http_code}\n' https://lebaron.sh/
curl -s https://lebaron.sh/ | grep -o '<title>.*</title>'
ssh -p7522 root@51.91.99.155 'jls; service keel_controlplane status; service keel_agentd status'
```
