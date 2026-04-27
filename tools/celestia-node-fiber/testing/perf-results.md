# Fibre DA Perf Results

Output of `TestEvNode_FiberDA_Perf` at each notable point on
`fibre-experiment`. Reproduce with:

```sh
cd tools/celestia-node-fiber
go test -tags fibre -timeout 6m -v -run TestEvNode_FiberDA_Perf ./testing/
```

Hardware: macOS arm64, `darwin/24.0.0`. Numbers are single-run samples,
not averages — directional, not benchmarks of record.

## Workload

- ev-node aggregator with `BatchingStrategy=immediate`,
  `BlockTime=200ms`, `DA.BlockTime=1s`, `ScrapeInterval=100ms`.
- Pump: 1000 txs/s × 10 KiB = 9.77 MiB/s for 30 s.
- DA backend: in-process Fibre adapter against single-validator
  celestia-app testnode.

## Baseline — fibre-experiment HEAD before fiber_client.go fixes

Commit: `6928a2cf` (julien/fiber + julien/speedup-submitter +
perf benchmark + escrow + scrape-interval test helper).

```
PERF: blocks=169 txs_executed=30000 injected_txs=30000
      injected_mb=292.97 pump_rate_mb_s=9.77
      da_blobs=120 da_total_mb=293.09 da_throughput_mb_s=9.77
      wall_s=30.00 gap_p50_s=0.238 gap_p99_s=0.432
```

Notes:
- 1:1 byte preservation pump → DA.
- Throughput == pump rate → run was input-limited, not Fibre-limited.
- Inter-blob gap p50 ≈ 240 ms, p99 ≈ 430 ms.

## After — fibre-experiment HEAD after all fixes

Commit: `81aedf6d` (fiber_client bugs fixed, Fiber-tuned profile,
120 MiB blob cap, real per-stream upload concurrency, syncer P2P
worker gated).

```
PERF: blocks=169 txs_executed=30000 injected_txs=30000
      injected_mb=292.97 pump_rate_mb_s=9.77
      da_blobs=18 da_total_mb=293.08 da_throughput_mb_s=9.77
      wall_s=30.00 gap_p50_s=1.759 gap_p99_s=2.088
```

| metric | before | after | delta |
|---|---|---|---|
| txs/sec executed | 1000 | 1000 | flat |
| MiB/sec to DA | 9.77 | 9.77 | flat |
| DA blobs | 120 | 18 | **−85%** |
| MiB / blob | 2.4 | 16.3 | **+6.7×** |
| inter-arrival p50 | 238 ms | 1759 ms | matches new BatchMaxDelay=1.5 s |
| inter-arrival p99 | 432 ms | 2088 ms | adaptive deadline + batching |
| 1:1 byte preservation | yes | yes | unchanged |

Reading: total throughput is unchanged because the **input pump is
the bottleneck at 9.77 MiB/s** — well below what Fibre can absorb
post-fixes. The improvement shows up in batching efficiency (fewer,
larger blobs) and pipeline headroom: with 4 concurrent uploads per
stream × 120 MiB / ~1.5 s, the new ceiling is ≈ 320 MiB/s per stream
vs the baseline's ~2 MiB/s ceiling (single worker × 5 MiB / ~1.5 s).
A higher pump rate is needed to actually surface the new ceiling.
