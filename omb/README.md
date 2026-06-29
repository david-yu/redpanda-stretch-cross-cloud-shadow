# OMB-equivalent sub-1-Mbps load generator for the cross-cloud StretchCluster

Two Kubernetes Jobs on rp-aws (`omb-producer` + `omb-consumer`) that drive **~0.8 Mbps of producer + consumer traffic** against the StretchCluster's `load-test` topic. The producer uses `kafka-producer-perf-test.sh` with `--throughput 25 --record-size 4096`, the consumer uses `kafka-consumer-perf-test.sh`, both running plaintext (broker TLS is off — see [`aws/manifests/stretchcluster.yaml`](../aws/manifests/stretchcluster.yaml)).

> **Why so low.** This rate was dialed down to **under 1 Mbps** for the shadow-link demo — the goal there is to watch shadow replication lag and confirm the link's fetch path stays region-local in AWS, not to stress the cluster. Bump it back up via the [tuning table](#tuning-the-rate) for throughput/ceiling tests.

> **Why `OMB` but `kafka-*-perf-test` under the hood.** Same reasoning as in the [same-cloud beta `omb/` notes](https://github.com/david-yu/redpanda-operator-stretch-beta/tree/main/omb): OMB-on-VM adds cross-cloud routing, driver config rendering, and a separate VM provisioning step for no extra information beyond what `--print-metrics` already prints. The bundled Kafka perf scripts run as a `Job` inside the `redpanda` namespace, so the load originates from the same cluster the controller is pinned to (rp-aws) and exercises the same cross-cloud RF=5 replication path that broker-to-broker traffic does.

## What it does

| Component | Image | What it runs |
|---|---|---|
| `omb-producer` Job | `apache/kafka:3.8.0` | `kafka-producer-perf-test.sh --topic load-test --record-size 4096 --throughput 25` ≈ **0.8 Mbps** |
| `omb-consumer` Job | `apache/kafka:3.8.0` | `kafka-consumer-perf-test.sh --topic load-test --group omb-consumer` — drains at producer rate |

`load-test` is created with **24 partitions × RF=5** so leaders distribute across all 3 clouds and any single-cloud loss still leaves quorum. Both Jobs run in the `redpanda` namespace and bootstrap from `redpanda.redpanda.svc.cluster.local:9092` over plaintext.

## Run

After steps 1-9 of the root README are complete (StretchCluster healthy, cross-cloud produce/consume validated):

```bash
./scripts/install-omb.sh
```

The script creates the topic and applies both Jobs. It prints the producer + consumer log-tail commands and the Console / Grafana hints.

## Tuning the rate

Change `--throughput` and `--record-size` in `producer-job.yaml`:

| `--throughput` × `--record-size` | Approx bandwidth |
|---|---|
| `25 × 4096` | **~0.8 Mbps (this repo's current default — sub-1-Mbps shadow-link demo)** |
| `1280 × 1024` | ~1.3 MB/s (10 Mbps — same as the same-cloud beta default) |
| `7680 × 4096` | ~30 MB/s (240 Mbps — the prior throughput-demo default) |
| `15360 × 4096` | ~60 MB/s |
| `-1 × 4096` | unbounded — useful for "what's the cluster's ceiling under cross-cloud RF=5?" probes |

If you bump throughput above ~60 MB/s on the 2× m5.xlarge AWS nodes, raise the producer Job's CPU limit beyond `2` first or the perf-test process becomes the bottleneck before the cluster does.

## Stop

```bash
kubectl --context rp-aws -n redpanda delete -f omb/producer-job.yaml -f omb/consumer-job.yaml
```

## Watching the workload

- **Console** (after `./scripts/install-console.sh`): Topics → `load-test` shows partition distribution, leader spread across rack labels (`aws` / `gcp` / `azure`), and consumer-group lag.
- **Grafana** (after `./scripts/install-monitoring.sh`): the bundled Redpanda dashboard breaks down produce / consume throughput, end-to-end latency, and partition leadership by rack — that's where a region failover becomes visible as leader migration + a brief consumer lag spike.
- **CLI**: `kubectl --context rp-aws -n redpanda logs -f job/omb-producer` for the per-5s `records sent / records/sec / avg latency / max latency` lines.
