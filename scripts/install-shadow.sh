#!/usr/bin/env bash
# scripts/install-shadow.sh — deploy the standalone shadow cluster on the
# rp-shadow EKS cluster (us-east-1) and create a REGION-LOCAL shadow link
# from the stretch cluster's AWS brokers (rp-aws) into it.
#
# Region-local: the shadow cluster lives in us-east-1, peered to the rp-aws
# VPC (shadow/terraform/), and the shadow link bootstraps against the rp-aws
# brokers ONLY. Because the stretch cluster pins all partition leaders to the
# AWS rack and shadow linking pulls from leaders, replication traffic stays
# inside us-east-1 on the AWS backbone.
#
# Prereqs:
#   - rp-shadow EKS up (shadow/terraform applied) + kubeconfig context
#     `rp-shadow` loaded.
#   - Stretch cluster healthy (root README steps 1-8) with an internal Kafka
#     endpoint reachable from the shadow VPC. Pass it via --stretch-kafka or
#     STRETCH_KAFKA (e.g. an internal-NLB external listener on rp-aws).
#
# Usage:
#   ./scripts/install-shadow.sh --license path/to/redpanda.license \
#       --stretch-kafka <host:port>[,<host:port>...]
#
# Env (optional):
#   SHADOW_CTX   (default rp-shadow)   STRETCH_CTX (default rp-aws)
#   NAMESPACE    (default redpanda)    LINK_NAME   (default stretch-to-shadow-dr)
#   CHART_VERSION (default: newest redpanda-data/redpanda chart)

set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)

SHADOW_CTX=${SHADOW_CTX:-rp-shadow}
STRETCH_CTX=${STRETCH_CTX:-rp-aws}
NAMESPACE=${NAMESPACE:-redpanda}
LINK_NAME=${LINK_NAME:-stretch-to-shadow-dr}
CHART_VERSION=${CHART_VERSION:-}
LICENSE_PATH=""
STRETCH_KAFKA=${STRETCH_KAFKA:-}

while [[ $# -gt 0 ]]; do
  case $1 in
    --license)       LICENSE_PATH=$2; shift 2 ;;
    --stretch-kafka) STRETCH_KAFKA=$2; shift 2 ;;
    -h|--help)       sed -n '2,/^set -uo/p' "$0" | sed 's/^# \{0,1\}//;s/^#$//'; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

log()  { echo "[install-shadow] $*" >&2; }
die()  { echo "[install-shadow] ERROR: $*" >&2; exit 1; }

command -v kubectl >/dev/null || die "kubectl not found"
command -v helm    >/dev/null || die "helm not found"

[[ -n "$LICENSE_PATH" && -f "$LICENSE_PATH" ]] || die "--license <path> is required and must be readable (shadow linking is Enterprise; both clusters need a license)"
[[ -n "$STRETCH_KAFKA" ]] || die "--stretch-kafka <host:port> is required (the rp-aws brokers' Kafka endpoint reachable from the shadow VPC over the peering)"

kubectl --context "$SHADOW_CTX" get ns >/dev/null 2>&1 || die "kube-context '$SHADOW_CTX' not reachable — apply shadow/terraform and load the context first"

# --- 1. namespace + license secret on the shadow cluster ---
log "ensuring namespace '$NAMESPACE' + license secret on $SHADOW_CTX"
kubectl --context "$SHADOW_CTX" create ns "$NAMESPACE" --dry-run=client -o yaml | kubectl --context "$SHADOW_CTX" apply -f - >/dev/null
kubectl --context "$SHADOW_CTX" -n "$NAMESPACE" create secret generic redpanda-license \
  --from-file="license.key=$LICENSE_PATH" --dry-run=client -o yaml | kubectl --context "$SHADOW_CTX" apply -f - >/dev/null

# --- 2. helm install the standalone shadow Redpanda ---
helm repo add redpanda-data https://charts.redpanda.com --force-update >/dev/null 2>&1 || true
helm repo update redpanda-data >/dev/null 2>&1 || true

VERSION_FLAG=()
if [[ -n "$CHART_VERSION" ]]; then
  VERSION_FLAG=(--version "$CHART_VERSION")
  log "using pinned redpanda chart version $CHART_VERSION"
else
  log "using newest redpanda-data/redpanda chart (must ship Redpanda >= 25.3 for shadow linking)"
fi

log "helm upgrade --install redpanda (3 brokers) on $SHADOW_CTX"
helm --kube-context "$SHADOW_CTX" upgrade --install redpanda redpanda-data/redpanda \
  -n "$NAMESPACE" "${VERSION_FLAG[@]}" \
  -f "$REPO_ROOT/shadow/helm-values/values-rp-shadow.yaml" \
  --wait --timeout 15m || die "shadow Redpanda helm install failed"

# Pick a shadow broker pod for rpk exec.
SHADOW_POD=$(kubectl --context "$SHADOW_CTX" -n "$NAMESPACE" get pod \
  -l app.kubernetes.io/name=redpanda -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
[[ -n "$SHADOW_POD" ]] || die "no shadow broker pod found on $SHADOW_CTX"

# --- 3. enable shadow linking on the shadow (destination) cluster ---
log "enabling enable_shadow_linking=true on the shadow cluster"
kubectl --context "$SHADOW_CTX" -n "$NAMESPACE" exec "$SHADOW_POD" -c redpanda -- \
  rpk cluster config set enable_shadow_linking true 2>&1 | sed 's/^/  /' || true

# --- 4. probe reachability of the stretch Kafka endpoint from the shadow side ---
PROBE_HOST=${STRETCH_KAFKA%%,*}; PROBE_HP=${PROBE_HOST##*@}
log "probing stretch Kafka endpoint '$PROBE_HP' from a shadow broker (region-local over VPC peering)"
kubectl --context "$SHADOW_CTX" -n "$NAMESPACE" exec "$SHADOW_POD" -c redpanda -- \
  rpk cluster info --brokers "$STRETCH_KAFKA" 2>&1 | sed 's/^/  /' \
  || log "WARN: could not query the stretch cluster from the shadow pod — check VPC peering routes + the rp-aws node SG rule (shadow/terraform/peering.tf) before continuing"

# --- 5. render + create the shadow link ---
RENDERED=$(mktemp)
sed "s|__STRETCH_AWS_KAFKA_ENDPOINT__|$STRETCH_KAFKA|g" \
  "$REPO_ROOT/shadow/shadow-link.yaml" > "$RENDERED"
# rpk wants a list item per broker; if STRETCH_KAFKA has commas, expand them.
if [[ "$STRETCH_KAFKA" == *,* ]]; then
  python3 - "$RENDERED" "$STRETCH_KAFKA" <<'PY' 2>/dev/null || true
import sys
path, csv = sys.argv[1], sys.argv[2]
hosts = csv.split(",")
block = "\n".join(f'    - "{h}"' for h in hosts)
txt = open(path).read().replace(f'    - "{csv}"', block)
open(path, "w").write(txt)
PY
fi

log "copying shadow-link config into $SHADOW_POD and creating link '$LINK_NAME'"
kubectl --context "$SHADOW_CTX" -n "$NAMESPACE" cp "$RENDERED" "$SHADOW_POD:/tmp/shadow-link.yaml" -c redpanda
kubectl --context "$SHADOW_CTX" -n "$NAMESPACE" exec "$SHADOW_POD" -c redpanda -- \
  rpk shadow create -c /tmp/shadow-link.yaml --no-confirm 2>&1 | sed 's/^/  /'
rm -f "$RENDERED"

cat >&2 <<EOF

============================================================
  Shadow link '$LINK_NAME' created on $SHADOW_CTX
============================================================

  Source (stretch) Kafka: $STRETCH_KAFKA   (rp-aws brokers, region-local)
  Shadow cluster:         $SHADOW_CTX / namespace $NAMESPACE

  Watch link + per-topic lag:
    kubectl --context $SHADOW_CTX -n $NAMESPACE exec $SHADOW_POD -c redpanda -- \\
      rpk shadow status $LINK_NAME
    kubectl --context $SHADOW_CTX -n $NAMESPACE exec $SHADOW_POD -c redpanda -- \\
      rpk shadow describe $LINK_NAME

  Confirm the shadowed load-test topic appears on the shadow cluster:
    kubectl --context $SHADOW_CTX -n $NAMESPACE exec $SHADOW_POD -c redpanda -- \\
      rpk topic list

  Region-locality check (replication traffic should be us-east-1 only):
    see README "Shadow link: verifying region-local traffic".
EOF
