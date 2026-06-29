# Redpanda Shadow Link satellite for the cross-cloud StretchCluster

A **region-local disaster-recovery satellite** for the cross-cloud Redpanda
StretchCluster in
[`redpanda-operator-stretch-cross-cloud-beta`](https://github.com/david-yu/redpanda-operator-stretch-cross-cloud-beta).

It stands up a **separate Redpanda cluster in AWS `us-east-1`** (`rp-shadow`,
its own EKS) and wires a **Shadow Link** from the stretch cluster into it, so
the shadow cluster continuously pulls topics, configs, and consumer offsets for
disaster recovery — **without any replication byte leaving `us-east-1`**.

> [!IMPORTANT]
> This repo is an **add-on**. Bring up the 3-cloud StretchCluster first (steps
> 1–9 of the [base scaffold](https://github.com/david-yu/redpanda-operator-stretch-cross-cloud-beta)).
> The shadow satellite then attaches to the `rp-aws` member of that cluster.

> [!NOTE]
> **Validation status (live run).** The **region-local data path is proven** and
> **OMB runs under 1 Mbps**. The **managed Shadow Link**, however, does *not*
> fully activate against a 3-cloud source from a single peered VPC: its
> control-plane tasks probe *every* source broker, and the gcp/azure brokers
> aren't routable from the shadow VPC. See [Validation](#validation) for the
> root cause and the two ways to get a fully-active link (single-region source,
> or join the shadow to the cross-cloud mesh). The region-locality design itself
> is sound — the gap is broker reachability, not the leader-pinning mechanism.

## Contents
- [Why this is region-local](#why-this-is-region-local)
- [Architecture](#architecture)
- [Repo layout](#repo-layout)
- [Prerequisites](#prerequisites)
- [Step-by-step](#step-by-step)
- [Validation](#validation)
- [Cost model](#cost-model)
- [Teardown](#teardown)
- [Caveats / known issues](#caveats--known-issues)

## Why this is region-local

The stretch cluster spans AWS `us-east-1`, GCP `us-east1`, and Azure `eastus`,
but it **pins all partition leaders to the AWS rack**:

```yaml
# aws/manifests/stretchcluster.yaml (base scaffold)
default_leaders_preference: "ordered_racks:aws,gcp,azure"
```

Shadow linking is **pull-based and fetches from each partition's leader**. So
if the shadow cluster lives in `us-east-1` and connects to the **rp-aws
brokers**, every replicated byte is fetched from an AWS-resident leader and
travels **AWS → AWS inside `us-east-1`** — never across a cloud boundary, never
over the public internet.

Two pieces make that connection work while keeping it on the AWS backbone:

1. **VPC peering** between the shadow VPC (`10.40.0.0/16`) and the rp-aws VPC
   (`10.10.0.0/16`), both in `us-east-1`. Traffic rides the AWS backbone.
2. A **NodePort external Kafka listener** on the rp-aws brokers, advertised on
   the brokers' **node InternalIPs** (`10.10.x`), which are routable from the
   shadow VPC over the peering. Shadow-side CoreDNS resolves the advertised
   broker names to those node IPs.

> Shadow linking has **no `client.rack`/follower-fetch knob**, so follower
> fetching is *not* the mechanism here — leader pinning + an AWS-local
> bootstrap is what guarantees region-locality.

## Architecture

```
                        AWS us-east-1
   ┌─────────────────────────────────────────────────────────────────┐
   │                                                                   │
   │   rp-aws VPC 10.10.0.0/16            rp-shadow VPC 10.40.0.0/16    │
   │   ┌───────────────────────┐         ┌──────────────────────────┐ │
   │   │ EKS rp-aws (stretch)   │  VPC    │ EKS rp-shadow             │ │
   │   │  brokers 0,1 (rack=aws)│◀═peering═▶ redpanda 0,1,2 (RF=3)   │ │
   │   │  ALL load-test leaders │ (active)│  Shadow Link consumer    │ │
   │   │  NodePort :31092 ──────┼─────────┼─▶ fetch from leaders     │ │
   │   └───────────┬───────────┘         └──────────────────────────┘ │
   │               │ Cilium ClusterMesh + IPsec VPN                    │
   └───────────────┼───────────────────────────────────────────────── ┘
                   │ (cross-cloud, base scaffold only)
        ┌──────────┴──────────┐
     GCP us-east1          Azure eastus
     brokers 3,4           broker 2
     (rack=gcp)            (rack=azure)

   Shadow-link fetch path  = rp-shadow ─▶ rp-aws node IP:31092 (peering)
                           = 100% inside us-east-1, AWS backbone
```

The shadow cluster is a **plain single-region Redpanda** (deployed with the
`redpanda` Helm chart, not the multicluster operator). It is **not** part of the
Cilium ClusterMesh or the StretchCluster — it only needs to reach the rp-aws
brokers' Kafka API.

## Repo layout

```
shadow/
  terraform/          dedicated rp-shadow EKS + VPC + peering to rp-aws (reads
                      ../../aws/terraform state for the peer VPC/route tables)
  helm-values/        values-rp-shadow.yaml (3-broker standalone cluster,
                      enable_shadow_linking=true, plaintext, ebs-sc)
  manifests/
    stretch-aws-external-kafka.yaml   merge-patch: enable a NodePort external
                                      Kafka listener on the StretchCluster
                                      (apply to ALL THREE cluster contexts)
    stretch-aws-kafka-nodeport.yaml   NodePort Service (externalTrafficPolicy:
                                      Local) exposing :31092 on the rp-aws nodes
  shadow-link.yaml    rpk ShadowLinkConfig (bootstrap = rp-aws node IPs:31092)
scripts/
  install-shadow.sh   deploy shadow Redpanda + create the shadow link
omb/                  OMB load generator, throttled to ~0.8 Mbps (<1 Mbps)
```

## Prerequisites

- The base cross-cloud StretchCluster up and healthy (`rpk cluster health` =
  `Healthy: true`, 5 brokers across aws/gcp/azure). The shadow stack reads
  `aws/terraform/terraform.tfstate` for the peer VPC + route tables.
- `rp-aws` / `rp-gcp` / `rp-azure` kube-contexts loaded; AWS authenticated.
- A Redpanda **Enterprise license** (shadow linking is an Enterprise feature;
  both clusters need it). Redpanda **v25.3+** on both clusters.
- `terraform`, `kubectl`, `helm`, `rpk`, `aws` CLIs.

## Step-by-step

### 1. Provision the shadow EKS + VPC peering

```bash
terraform -chdir=shadow/terraform init
terraform -chdir=shadow/terraform apply        # ~12 min; EKS + VPC + peering
aws eks update-kubeconfig --region us-east-1 --name rp-shadow --alias rp-shadow
```

This creates the shadow VPC (`10.40.0.0/16`), a 3-node EKS cluster, the
`rp-shadow ↔ rp-aws` VPC peering connection, the cross-VPC routes on **both**
sides, and an all-traffic ingress rule from the shadow CIDR on the rp-aws node
security group.

### 2. Deploy the standalone shadow Redpanda

```bash
kubectl --context rp-shadow create ns redpanda
kubectl --context rp-shadow -n redpanda create secret generic redpanda-license \
  --from-file=license.key=/path/to/redpanda.license

helm --kube-context rp-shadow upgrade --install redpanda redpanda-data/redpanda \
  -n redpanda --version 26.1.6 -f shadow/helm-values/values-rp-shadow.yaml --wait
```

### 3. Expose the stretch rp-aws Kafka API to the shadow VPC

**3a — enable the NodePort external Kafka listener (ALL THREE contexts).** The
StretchCluster operator enforces an identical `.spec` across member clusters, so
the patch must be applied everywhere or reconciliation stalls on
`SpecSynced=False`:

```bash
for ctx in rp-aws rp-gcp rp-azure; do
  kubectl --context $ctx -n redpanda patch stretchcluster redpanda \
    --type merge --patch-file shadow/manifests/stretch-aws-external-kafka.yaml
done
```

**3b — create the NodePort Service on rp-aws** (the operator doesn't create one
in flat mode). `externalTrafficPolicy: Local` makes each node IP map to its own
local broker:

```bash
kubectl --context rp-aws -n redpanda apply -f shadow/manifests/stretch-aws-kafka-nodeport.yaml
```

**3c — point shadow CoreDNS at the rp-aws node IPs.** The brokers advertise
their pod hostnames (`redpanda-rp-aws-0`, `redpanda-rp-aws-1`); map those to the
node InternalIPs the brokers run on (look them up with
`kubectl --context rp-aws -n redpanda get pods -o wide`):

```
# add to the shadow cluster's kube-system/coredns Corefile, before `kubernetes`:
hosts {
    10.10.13.169 redpanda-rp-aws-0 redpanda-rp-aws-0.redpanda.svc.cluster.local
    10.10.26.87  redpanda-rp-aws-1 redpanda-rp-aws-1.redpanda.svc.cluster.local
    fallthrough
}
```

### 4. Create the shadow link

Set `bootstrap_servers` in `shadow/shadow-link.yaml` to the rp-aws node
IPs on `:31092`, then:

```bash
kubectl --context rp-shadow -n redpanda cp shadow/shadow-link.yaml redpanda-0:/tmp/shadow-link.yaml -c redpanda
kubectl --context rp-shadow -n redpanda exec redpanda-0 -c redpanda -- \
  rpk shadow create -c /tmp/shadow-link.yaml --no-confirm
```

`scripts/install-shadow.sh` wraps steps 2 + 4 (pass `--license` and
`--stretch-kafka 10.10.x:31092,10.10.y:31092`).

### 5. Reduce OMB to under 1 Mbps

`omb/producer-job.yaml` is throttled to `--throughput 25 --record-size 4096`
≈ **0.82 Mbps**. Start it on the stretch cluster with the base scaffold's
`scripts/install-omb.sh`.

## Validation

Confirmed on a live run (Redpanda v26.1, RF=5 stretch / RF=3 shadow). Two of
the three claims are fully proven; the third surfaced an important constraint of
shadow-linking a **cross-cloud** source — read it before relying on this for DR.

- ✅ **Region-local data path — PROVEN.** A consume issued from inside a shadow
  broker pod against the rp-aws node IPs returned real `load-test` records,
  proving the hop `rp-shadow → rp-aws node IP:31092 → leader` works entirely
  over the VPC peering inside `us-east-1`:
  ```
  getent hosts redpanda-rp-aws-0  =>  10.10.13.169
  rpk topic consume load-test --brokers 10.10.13.169:31092,10.10.26.87:31092
    => {"topic":"load-test","partition":7,"offset":801, ...}   # live OMB data
  ```
- ✅ **OMB throughput.** Steady `24.9–25.1 records/sec, 0.10 MB/sec`
  (~0.82 Mbps), comfortably under the 1 Mbps target.
- ⚠️ **Managed Shadow Link — created/ACTIVE, but topic-sync blocked against a
  cross-cloud source.** `rpk shadow create` succeeded and the link reports
  `STATE ACTIVE`, but its `Source Topic Sync` task sits `LINK_UNAVAILABLE`:
  ```
  Source Topic Sync ... LINK_UNAVAILABLE
    Failed to get supported API version for describe_configs:
    { node: 4 }, { error_code: broker_not_available [8] }
  ```
  The link's control-plane tasks (topic-config sync, and consumer-group sync
  when enabled) **probe every source broker**, including the gcp (node 4) and
  azure (node 2) brokers. Those brokers advertise their *own* clouds' node IPs
  (`10.20.x` / `10.30.x`), which are **not routable from the dedicated shadow
  VPC** (peered only to rp-aws). Pausing offset + security sync and scoping the
  link to `load-test` did **not** clear it — the topic-sync API-version probe to
  the unreachable brokers blocks first.

### What this means

The **region-locality goal is sound and proven** (leader pinning keeps the data
fetch on aws). The limitation is purely **reachability**: a managed Shadow Link
against a *3-cloud* source needs to reach *all* source brokers, and a shadow in
a single peered VPC reaches only the aws ones. Two ways to get a fully-active
managed link:

1. **Single-region source** — point the shadow at a single-cloud Redpanda
   cluster (e.g. the [same-cloud beta](https://github.com/david-yu/redpanda-operator-stretch-beta)).
   The peered-VPC + NodePort exposure here then reaches *all* brokers and the
   link activates fully.
2. **Put the shadow on the cross-cloud network** — join the shadow EKS to the
   Cilium ClusterMesh (or extend the IPsec VPN mesh to the shadow VPC) so it can
   reach the gcp/azure broker addresses for control-plane ops. Data fetch still
   comes from the aws-pinned leaders, so it stays region-local; only tiny
   control traffic crosses clouds. This trades the "simple dedicated VPC" model
   for mesh membership.

## Cost model

All figures are **AWS `us-east-1` on-demand list price** rough estimates; round
to your committed-use/Savings-Plan discounts. The headline: the shadow
satellite's *networking* cost is dominated by whether replication is
region-local — which is the whole point of this design.

### Shadow satellite — infrastructure (hourly, while running)

| Component | Spec | ~$/hr | ~$/mo (730h) |
|---|---|---|---|
| EKS control plane | 1 cluster | $0.10 | $73 |
| Worker nodes | 3 × `m5.xlarge` | $0.576 | $420 |
| EBS (broker PVCs) | 3 × 50 GiB gp3 | $0.016 | $12 |
| NAT gateway | 1 (single-AZ) | $0.045 | $33 |
| VPC peering | hourly | **$0.00** | **$0.00** |
| **Shadow infra total** | | **~$0.74/hr** | **~$538/mo** |

> Peering itself has **no hourly charge** — you only pay for data crossing it
> (below). Drop to 1 × `m5.large` broker (RF-1 demo) to roughly halve the node
> line.

### Networking — the region-local payoff

Shadow replication volume = the source produce rate the link must pull. At the
demo's **0.8 Mbps** that's tiny; the table also shows a **30 MB/s** production
workload so the *rate-driven* costs are visible.

| Replication path | $/GB | @ 0.8 Mbps (~0.26 TB/mo) | @ 30 MB/s (~9.5 TB/day) |
|---|---|---|---|
| **Region-local (this design)** — VPC peering, intra-region, cross-AZ | ~$0.01–0.02 | **~$3–5/mo** | **~$0.10–0.19/hr** |
| If the shadow were in **another cloud** — internet egress | $0.087–0.12 | ~$23–31/mo | ~$0.83–1.15/hr |
| If the shadow were in **another AWS region** — inter-region transfer | ~$0.02 | ~$5/mo | ~$0.19/hr |

> At 30 MB/s the cross-cloud path would cost **~$600–830/hr** in egress alone —
> region-locality is a **5–10×** saving on the data-transfer line, and the gap
> grows linearly with throughput. Keeping the shadow in `us-east-1` next to the
> pinned leaders is what captures it.

### Base cross-cloud StretchCluster (context)

The 3-cloud base scaffold is the expensive part, mostly **cross-cloud egress**:
for the original 30 MB/s demo it runs **~$29/hr of egress** across the
AWS/GCP/Azure mesh (see the base repo's
[Cost section](https://github.com/david-yu/redpanda-operator-stretch-cross-cloud-beta#cost)),
plus ~3 × the per-cloud compute/EKS-GKE-AKS + 3 VPN gateways
(AWS VGW/GCP HA-VPN/Azure VPN-GW ≈ $0.05–0.19/hr each). The shadow satellite
adds **~$0.74/hr infra + a few $/hr of region-local transfer** on top — a small
increment precisely *because* its traffic never joins the cross-cloud egress
that dominates the base cost.

## Teardown

Tear the shadow tier down **before** the base scaffold's `rp-aws` — the shadow
stack owns the VPC peering + the routes/SG rule it wrote into the rp-aws VPC
(the base repo's `scripts/teardown.sh` already sequences this when
`shadow/terraform` state is present):

```bash
kubectl --context rp-shadow -n redpanda exec redpanda-0 -c redpanda -- \
  rpk shadow delete stretch-to-shadow-dr --no-confirm
helm --kube-context rp-shadow uninstall redpanda -n redpanda
terraform -chdir=shadow/terraform destroy
# then run the base scaffold teardown for the 3 stretch clusters
```

## Caveats / known issues

- **Advertised addresses are pod hostnames.** In flat cross-cluster mode the
  operator advertises the external listener as `redpanda-rp-aws-<n>` (pod
  hostname), and does **not** create a NodePort/LB Service. Hence the manual
  NodePort Service + shadow-side CoreDNS `hosts` mapping. This is the crux of
  the wiring; everything else follows from it.
- **Broker→node pinning.** The CoreDNS `hosts` entries map each broker name to
  the node IP it currently runs on. If an aws broker reschedules to a different
  node, update the mapping (or use a per-broker internal NLB + a Route53 private
  zone for a self-healing variant).
- **Leader pinning is what keeps it region-local.** If leadership ever falls
  through to GCP/Azure (e.g. an `us-east-1` outage), the shadow link would need
  to reach those brokers — whose advertised node IPs are *not* routable from the
  shadow VPC. That's acceptable for DR (you'd be failing over anyway), but it
  means region-locality is a steady-state property, not a hard guarantee during
  a regional outage.
- **Plaintext.** Matches the base scaffold (broker TLS is off; the hop is on the
  AWS backbone inside one region). Add TLS/SASL on the external listener for any
  non-demo use.
- **Enterprise + v25.3+** required on both clusters for shadow linking.

---
*Companion to [redpanda-operator-stretch-cross-cloud-beta](https://github.com/david-yu/redpanda-operator-stretch-cross-cloud-beta). Generated from a validated live run.*
