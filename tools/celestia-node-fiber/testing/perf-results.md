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
