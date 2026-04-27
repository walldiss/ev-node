//go:build fibre

package cnfibertest_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/celestia-node/api/client"

	"github.com/evstack/ev-node/block"
	datypes "github.com/evstack/ev-node/pkg/da/types"

	cnfiber "github.com/evstack/ev-node/tools/celestia-node-fiber"
	cnfibertest "github.com/evstack/ev-node/tools/celestia-node-fiber/testing"
)

// perfConfig parametrizes the perf benchmark. Each test wires up its own
// config and calls runFiberPerfTest below; the helper does the actual
// chain setup, pump, and recording.
type perfConfig struct {
	label        string        // PERF: line prefix used for grep'ing CI output
	txSize       int           // bytes per injected tx
	txsPerTick   int           // txs injected per pumpInterval
	pumpInterval time.Duration // ticker period
	runDuration  time.Duration // total pump window
}

// pumpRateMBps returns the configured input rate in MiB/s.
func (c perfConfig) pumpRateMBps() float64 {
	bps := float64(c.txSize*c.txsPerTick) / c.pumpInterval.Seconds()
	return bps / (1024 * 1024)
}

// TestEvNode_FiberDA_Perf is the moderate-rate perf benchmark used as the
// baseline for before/after comparisons on fibre-experiment. It pumps
// ~10 MiB/s of 10 KiB txs for 30 s.
//
// Run with:
//
//	cd tools/celestia-node-fiber
//	go test -tags fibre -timeout 6m -v -run TestEvNode_FiberDA_Perf$ ./testing/
func TestEvNode_FiberDA_Perf(t *testing.T) {
	if testing.Short() {
		t.Skip("perf benchmark skipped in short mode")
	}
	runFiberPerfTest(t, perfConfig{
		label:        "PERF",
		txSize:       10 * 1024,
		txsPerTick:   100,
		pumpInterval: 100 * time.Millisecond,
		runDuration:  30 * time.Second,
	})
}

// TestEvNode_FiberDA_Perf_Hyper pumps ~100 MiB/s of 10 KiB txs for 30 s
// (10 000 txs/s). 10 KiB matches an EVM-shaped tx; 100 KiB at the same
// MiB/s makes block production (marshal + sign + store) the bottleneck
// — observed during early hyper runs that produced only 1.4 blocks/s
// vs. the 5 blocks/s the BlockTime asks for.
//
// At 10 KiB × 10 000/s the per-block math is:
//
//	scrape  (100 ms) → 1000 txs × 10 KiB = ~10 MiB
//	block   (200 ms) → 2 scrapes        = ~20 MiB
//	5 blocks/s × ~20 MiB = 100 MiB/s of block-data production
//	submitter @ 1.5 s adaptive → 7–8 blocks × 20 MiB ≈ 140 MiB pending
//	→ trips the 96 MiB threshold; 4 concurrent workers ship in parallel
//
// So this configuration should saturate the post-fix DA pipeline
// without starving block production.
//
// Escrow draw at this rate ≈ 70 TIA — well within the test network's
// 50 000 TIA escrow.
//
//	go test -tags fibre -timeout 6m -v -run TestEvNode_FiberDA_Perf_Hyper ./testing/
func TestEvNode_FiberDA_Perf_Hyper(t *testing.T) {
	if testing.Short() {
		t.Skip("hyper perf benchmark skipped in short mode")
	}
	runFiberPerfTest(t, perfConfig{
		label:        "PERF_HYPER",
		txSize:       10 * 1024,
		txsPerTick:   1000,
		pumpInterval: 100 * time.Millisecond,
		runDuration:  30 * time.Second,
	})
}

// runFiberPerfTest stands up the chain + bridge + adapter, wires an
// ev-node aggregator pointed at the adapter, and pumps txs at the
// configured rate while recording BlobEvents from the data namespace.
// Prints a single machine-parseable "<label>: ..." line at the end.
//
// No hard pass/fail on numbers — only on operational errors (no events
// received, blocks not produced, node panic). Absolute throughput is
// machine-dependent.
func runFiberPerfTest(t *testing.T, cfg perfConfig) {
	t.Helper()
	t.Logf("starting perf run: label=%s tx_size=%d txs_per_tick=%d pump_interval=%s run_duration=%s target_pump_mb_s=%.2f",
		cfg.label, cfg.txSize, cfg.txsPerTick, cfg.pumpInterval, cfg.runDuration, cfg.pumpRateMBps())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	network := cnfibertest.StartNetwork(t, ctx)
	bridge := cnfibertest.StartBridge(t, ctx, network)

	adapter, err := cnfiber.New(ctx, cnfiber.Config{
		Client: client.Config{
			ReadConfig: client.ReadConfig{
				BridgeDAAddr: bridge.RPCAddr(),
				DAAuthToken:  bridge.AdminToken,
				EnableDATLS:  false,
			},
			SubmitConfig: client.SubmitConfig{
				DefaultKeyName: network.ClientAccount,
				Network:        "private",
				CoreGRPCConfig: client.CoreGRPCConfig{
					Addr: network.ConsensusGRPCAddr(),
				},
			},
		},
	}, network.Consensus.Keyring)
	require.NoError(t, err, "constructing adapter")
	t.Cleanup(func() { _ = adapter.Close() })

	// Subscribe to the DATA namespace before starting the node — that's
	// where the bulk bytes go and where throughput is measured. Header
	// blobs are tiny (~1 KiB) so we ignore them for throughput math.
	fullDataNS := datypes.NamespaceFromString(evnodeDataNS).Bytes()
	dataNSID := fullDataNS[len(fullDataNS)-10:]
	events, err := adapter.Listen(ctx, dataNSID, 0)
	require.NoError(t, err, "starting fiber Listen on data namespace")

	rollnode, exec, nodeCleanup := newFiberEvNode(t, ctx, adapter)
	t.Cleanup(nodeCleanup)

	nodeCtx, nodeCancel := context.WithCancel(ctx)
	t.Cleanup(nodeCancel)

	nodeErrCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				nodeErrCh <- fmt.Errorf("node panicked: %v", r)
			}
		}()
		nodeErrCh <- rollnode.Run(nodeCtx)
	}()

	require.Eventually(t, func() bool {
		return exec.Stats().BlocksProduced >= 1
	}, 60*time.Second, 200*time.Millisecond, "node should produce its first block")

	startBlocks := exec.Stats().BlocksProduced
	startTxs := exec.Stats().TotalExecutedTxs

	// Pre-generate a randomness pool sized for several txs. Sampling
	// from offsets within a fixed pool keeps the pump's CPU off the
	// CSPRNG hot path at high tx sizes — at 100 MiB/s, calling
	// rand.Read per tx becomes a measurable bottleneck.
	pool := make([]byte, max(8*cfg.txSize, 1<<20))
	if _, err := rand.Read(pool); err != nil {
		t.Fatalf("seeding random pool: %v", err)
	}

	var (
		injectedCount atomic.Uint64
		injectedBytes atomic.Uint64
	)

	pumpStart := time.Now()
	pumpDeadline := pumpStart.Add(cfg.runDuration)

	pumpWg := sync.WaitGroup{}
	pumpWg.Add(1)
	go func() {
		defer pumpWg.Done()
		ticker := time.NewTicker(cfg.pumpInterval)
		defer ticker.Stop()
		var seq uint64
		poolLen := len(pool)
		for {
			now := time.Now()
			if !now.Before(pumpDeadline) {
				return
			}
			for range cfg.txsPerTick {
				seq++
				// Each tx is a fresh slice so the executor's channel
				// can hold ownership without aliasing. The first 16
				// bytes are seq + emit-time so a future receiver can
				// reconstruct ordering or measure mempool latency;
				// the rest is sampled from the random pool — cheaper
				// than calling rand.Read per tx.
				cp := make([]byte, cfg.txSize)
				binary.BigEndian.PutUint64(cp, seq)
				binary.BigEndian.PutUint64(cp[8:], uint64(now.UnixNano()))
				offset := int(seq*7919) % (poolLen - cfg.txSize + 16)
				if offset < 0 {
					offset = 0
				}
				copy(cp[16:], pool[offset:offset+cfg.txSize-16])
				exec.InjectTx(cp)
				injectedCount.Add(1)
				injectedBytes.Add(uint64(cfg.txSize))
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	type blobReceipt struct {
		recvAt   time.Time
		dataSize uint64
		height   uint64
	}
	var (
		receipts   []blobReceipt
		receiptsMu sync.Mutex
	)
	recorderDone := make(chan struct{})
	go func() {
		defer close(recorderDone)
		for {
			select {
			case ev, ok := <-events:
				if !ok {
					return
				}
				receiptsMu.Lock()
				receipts = append(receipts, blobReceipt{
					recvAt:   time.Now(),
					dataSize: ev.DataSize,
					height:   ev.Height,
				})
				receiptsMu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	pumpWg.Wait()
	pumpEnd := time.Now()

	// Drain phase: keep listening past the last injected tx so in-
	// flight uploads have time to land. The Fibre testnode batches PFF
	// settlements and the BlobEvent delivery cadence can be ~10 s
	// between events under load, so the "no new events" early-exit
	// uses a generous 20 s window to avoid clipping the tail.
	const (
		drainTimeout = 90 * time.Second
		stableWindow = 20 * time.Second
	)
	drainDeadline := time.Now().Add(drainTimeout)
	for time.Now().Before(drainDeadline) {
		receiptsMu.Lock()
		nReceipts := len(receipts)
		receiptsMu.Unlock()
		time.Sleep(stableWindow)
		receiptsMu.Lock()
		if len(receipts) == nReceipts {
			receiptsMu.Unlock()
			break
		}
		receiptsMu.Unlock()
	}

	nodeCancel()
	select {
	case err := <-nodeErrCh:
		if err != nil && err != context.Canceled {
			t.Logf("node Run returned: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Logf("node did not exit within 15s after cancel")
	}

	endStats := exec.Stats()
	blocksProduced := endStats.BlocksProduced - startBlocks
	txsExecuted := endStats.TotalExecutedTxs - startTxs

	receiptsMu.Lock()
	finalReceipts := append([]blobReceipt(nil), receipts...)
	receiptsMu.Unlock()

	require.NotEmpty(t, finalReceipts, "expected at least one Fiber BlobEvent")

	var totalDABytes uint64
	for _, r := range finalReceipts {
		totalDABytes += r.dataSize
	}
	wallTime := pumpEnd.Sub(pumpStart)
	throughputMBps := float64(totalDABytes) / wallTime.Seconds() / (1024 * 1024)
	pumpedMB := float64(injectedBytes.Load()) / (1024 * 1024)
	pumpRateMBps := pumpedMB / wallTime.Seconds()

	sort.Slice(finalReceipts, func(i, j int) bool {
		return finalReceipts[i].recvAt.Before(finalReceipts[j].recvAt)
	})
	gaps := make([]float64, 0, len(finalReceipts)-1)
	for i := 1; i < len(finalReceipts); i++ {
		gaps = append(gaps, finalReceipts[i].recvAt.Sub(finalReceipts[i-1].recvAt).Seconds())
	}
	gapP50, gapP99 := percentile(gaps, 0.50), percentile(gaps, 0.99)

	txsPerSec := float64(txsExecuted) / wallTime.Seconds()

	t.Logf("%s: blocks=%d txs_executed=%d txs_per_sec=%.0f injected_txs=%d injected_mb=%.2f pump_rate_mb_s=%.2f "+
		"da_blobs=%d da_total_mb=%.2f da_throughput_mb_s=%.2f wall_s=%.2f gap_p50_s=%.3f gap_p99_s=%.3f",
		cfg.label, blocksProduced, txsExecuted, txsPerSec, injectedCount.Load(), pumpedMB, pumpRateMBps,
		len(finalReceipts), float64(totalDABytes)/(1024*1024), throughputMBps, wallTime.Seconds(),
		gapP50, gapP99)

	require.Greater(t, totalDABytes, uint64(0), "no DA bytes received via Fiber")
	require.Greater(t, blocksProduced, uint64(0), "no blocks produced during pump window")

	_ = block.FiberBlobEvent{} // keep block import alive
}

func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	idx := int(float64(len(cp)-1) * p)
	return cp[idx]
}
