#!/bin/sh
#
# Generates the private CA and per-identity leaf certificates this
# project's mutual TLS uses, and manages revocation and rotation on top
# of a real openssl ca(1) certificate database (needed because openssl
# ca -gencrl is the only tool that can produce a CRL). Every leaf
# certificate is signed by one CA and gets both serverAuth and
# clientAuth extended key usage. Validity defaults to 10 years,
# overridable with a trailing --days N.
#
# Usage:
#   ./scripts/gen-certs.sh init
#   ./scripts/gen-certs.sh node <name> <ip-address> [--days N]
#   ./scripts/gen-certs.sh client <name> [--days N]
#   ./scripts/gen-certs.sh revoke <name>
#   ./scripts/gen-certs.sh crl
#
# Reissuing an existing node/client name rotates it: the new certificate
# is issued first, and only once that succeeds is the previous
# certificate under that name revoked and crl.pem regenerated, so a
# failed reissue never strands an identity with zero valid certificates.
#
# Output goes to $GEN_CERTS_OUT_DIR (default: ./certs).

set -eu

OUT_DIR="${GEN_CERTS_OUT_DIR:-certs}"
DEFAULT_DAYS=3650

mkdir -p "$OUT_DIR"

fail() {
    echo "gen-certs: $*" >&2
    exit 1
}

cmd="${1:-}"
[ -n "$cmd" ] || fail "usage: $0 <init|node|client|revoke|crl> ..."
shift

ca_config() {
    echo "$OUT_DIR/ca-db/openssl.cnf"
}

init_ca_db() {
    if [ -f "$OUT_DIR/ca-db/index.txt" ]; then
        return
    fi
    mkdir -p "$OUT_DIR/ca-db/newcerts"
    : > "$OUT_DIR/ca-db/index.txt"
    echo "unique_subject = no" > "$OUT_DIR/ca-db/index.txt.attr"
    echo 1000 > "$OUT_DIR/ca-db/serial"
    echo 1000 > "$OUT_DIR/ca-db/crlnumber"
    cat > "$(ca_config)" <<CNFEOF
[ ca ]
default_ca = CA_default

[ CA_default ]
database         = $OUT_DIR/ca-db/index.txt
serial           = $OUT_DIR/ca-db/serial
crlnumber        = $OUT_DIR/ca-db/crlnumber
new_certs_dir    = $OUT_DIR/ca-db/newcerts
certificate      = $OUT_DIR/ca.crt
private_key      = $OUT_DIR/ca.key
default_md       = sha256
default_days     = $DEFAULT_DAYS
default_crl_days = $DEFAULT_DAYS
policy           = policy_anything
email_in_dn      = no
unique_subject   = no
copy_extensions  = none

[ policy_anything ]
countryName            = optional
stateOrProvinceName    = optional
organizationName       = optional
organizationalUnitName = optional
commonName             = supplied
emailAddress           = optional
CNFEOF
    openssl ca -config "$(ca_config)" -gencrl -out "$OUT_DIR/crl.pem"
    echo "gen-certs: initialized CA database at $OUT_DIR/ca-db and wrote empty $OUT_DIR/crl.pem"
}

issue_leaf() {
    name="$1"
    days="$2"
    san_line="$3"

    tmp_key="$OUT_DIR/.$name.key.tmp.$$"
    tmp_crt="$OUT_DIR/.$name.crt.tmp.$$"
    tmp_csr=$(mktemp)
    ext_file=$(mktemp)
    trap 'rm -f "$tmp_key" "$tmp_crt" "$tmp_csr" "$ext_file"' EXIT

    openssl genrsa -out "$tmp_key" 4096
    chmod 600 "$tmp_key"
    openssl req -new -key "$tmp_key" -subj "/CN=$name" -out "$tmp_csr"

    if [ -n "$san_line" ]; then
        printf 'subjectAltName = %s\nextendedKeyUsage = serverAuth, clientAuth\n' "$san_line" > "$ext_file"
    else
        printf 'extendedKeyUsage = serverAuth, clientAuth\n' > "$ext_file"
    fi

    openssl ca -config "$(ca_config)" -in "$tmp_csr" -out "$tmp_crt" \
        -days "$days" -batch -extfile "$ext_file" -notext
    rm -f "$tmp_csr" "$ext_file"

    if [ -f "$OUT_DIR/$name.crt" ]; then
        mv "$OUT_DIR/$name.crt" "$OUT_DIR/$name.crt.previous"
        mv "$tmp_key" "$OUT_DIR/$name.key"
        mv "$tmp_crt" "$OUT_DIR/$name.crt"
        trap - EXIT
        openssl ca -config "$(ca_config)" -revoke "$OUT_DIR/$name.crt.previous"
        openssl ca -config "$(ca_config)" -gencrl -out "$OUT_DIR/crl.pem"
        rm -f "$OUT_DIR/$name.crt.previous"
        echo "gen-certs: rotated $name (previous certificate revoked, crl.pem refreshed)"
    else
        mv "$tmp_key" "$OUT_DIR/$name.key"
        mv "$tmp_crt" "$OUT_DIR/$name.crt"
        trap - EXIT
    fi
}

case "$cmd" in
    init)
        days="$DEFAULT_DAYS"
        if [ "${1:-}" = "--days" ]; then
            days="${2:-}"
            [ -n "$days" ] || fail "--days requires a value"
        fi
        if [ -f "$OUT_DIR/ca.key" ]; then
            echo "gen-certs: $OUT_DIR/ca.key already exists, reusing it"
        else
            openssl genrsa -out "$OUT_DIR/ca.key" 4096
            chmod 600 "$OUT_DIR/ca.key"
            openssl req -x509 -new -nodes -key "$OUT_DIR/ca.key" -sha256 -days "$days" \
                -subj "/CN=keel-cluster-ca" -out "$OUT_DIR/ca.crt"
            echo "gen-certs: wrote $OUT_DIR/ca.crt and $OUT_DIR/ca.key"
        fi
        init_ca_db
        ;;
    node)
        name="${1:-}"
        addr="${2:-}"
        [ -n "$name" ] && [ -n "$addr" ] || fail "usage: $0 node <name> <ip-address> [--days N]"
        shift 2
        days="$DEFAULT_DAYS"
        if [ "${1:-}" = "--days" ]; then
            days="${2:-}"
            [ -n "$days" ] || fail "--days requires a value"
        fi
        [ -f "$OUT_DIR/ca.key" ] || fail "run '$0 init' first"
        issue_leaf "$name" "$days" "IP:$addr"
        echo "gen-certs: wrote $OUT_DIR/$name.crt and $OUT_DIR/$name.key (SAN=IP:$addr)"
        ;;
    client)
        name="${1:-}"
        [ -n "$name" ] || fail "usage: $0 client <name> [--days N]"
        shift
        days="$DEFAULT_DAYS"
        if [ "${1:-}" = "--days" ]; then
            days="${2:-}"
            [ -n "$days" ] || fail "--days requires a value"
        fi
        [ -f "$OUT_DIR/ca.key" ] || fail "run '$0 init' first"
        issue_leaf "$name" "$days" ""
        echo "gen-certs: wrote $OUT_DIR/$name.crt and $OUT_DIR/$name.key"
        ;;
    revoke)
        name="${1:-}"
        [ -n "$name" ] || fail "usage: $0 revoke <name>"
        [ -f "$OUT_DIR/$name.crt" ] || fail "no certificate found for '$name' at $OUT_DIR/$name.crt"
        openssl ca -config "$(ca_config)" -revoke "$OUT_DIR/$name.crt"
        openssl ca -config "$(ca_config)" -gencrl -out "$OUT_DIR/crl.pem"
        echo "gen-certs: revoked $name and refreshed $OUT_DIR/crl.pem"
        ;;
    crl)
        openssl ca -config "$(ca_config)" -gencrl -out "$OUT_DIR/crl.pem"
        echo "gen-certs: refreshed $OUT_DIR/crl.pem"
        ;;
    *)
        fail "unknown subcommand: $cmd (expected init|node|client|revoke|crl)"
        ;;
esac
