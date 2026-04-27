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

// TestEvNode_FiberDA_Perf measures end-to-end throughput and latency of
// ev-node's DA submission pipeline against a real Fibre adapter. It is
// intentionally simple — pump txs at a fixed rate for a fixed duration,
// listen on the data namespace, and report aggregate numbers.
//
// The test prints a single machine-parseable PERF: line at the end so a
// before/after diff (e.g. captured on fibre-experiment HEAD vs. HEAD~N)
// can be eyeballed or scripted. It does NOT make hard pass/fail
// assertions on numbers — it only fails on operational errors (node
// panics, no events received, etc.) so the absolute numbers can vary
// across machines without breaking CI.
//
// To run only this test:
//
//	cd tools/celestia-node-fiber
//	go test -tags fibre -timeout 5m -v -run TestEvNode_FiberDA_Perf ./testing/
//
// Use -short to skip (the chain + bridge spinup is ~30s).
func TestEvNode_FiberDA_Perf(t *testing.T) {
	if testing.Short() {
		t.Skip("perf benchmark skipped in short mode")
	}

	// Tunables. Sized so a default-build (5 MiB DefaultMaxBlobSize) ev-node
	// can keep up: per-block data ≈ 100 txs × 10 KiB = 1 MiB, well under
	// the 5 MiB cap. Scale up txSize/txsPerTick after rebuilding with the
	// 128 MiB ldflag.
	const (
		txSize       = 10 * 1024              // 10 KiB per tx
		txsPerTick   = 100                    // 100 txs per pump tick
		pumpInterval = 100 * time.Millisecond // → 1000 txs/s = 10 MiB/s input
		runDuration  = 30 * time.Second
	)

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

	// Wait until the node has produced at least one block before starting
	// the pump — keeps the start-of-run timing free of node-spinup noise.
	require.Eventually(t, func() bool {
		return exec.Stats().BlocksProduced >= 1
	}, 60*time.Second, 200*time.Millisecond, "node should produce its first block")

	startBlocks := exec.Stats().BlocksProduced
	startTxs := exec.Stats().TotalExecutedTxs

	// Pump goroutine: inject txs at a fixed rate for runDuration.
	// Tx layout: 8-byte sequence number + 8-byte unix-nano emit time +
	// random padding. The seq number gives the recorder a way to detect
	// drops; the timestamp would let a future, more thorough latency
	// breakdown attribute time spent in mempool vs. in-flight to DA.
	var (
		injectedCount atomic.Uint64
		injectedBytes atomic.Uint64
	)

	pumpStart := time.Now()
	pumpDeadline := pumpStart.Add(runDuration)

	pumpWg := sync.WaitGroup{}
	pumpWg.Add(1)
	go func() {
		defer pumpWg.Done()
		ticker := time.NewTicker(pumpInterval)
		defer ticker.Stop()
		var seq uint64
		buf := make([]byte, txSize)
		for {
			now := time.Now()
			if !now.Before(pumpDeadline) {
				return
			}
			for range txsPerTick {
				seq++
				binary.BigEndian.PutUint64(buf, seq)
				binary.BigEndian.PutUint64(buf[8:], uint64(now.UnixNano()))
				// Random padding so blob compressors (if any) don't get a
				// free ride and skew throughput numbers.
				if _, err := rand.Read(buf[16:]); err != nil {
					t.Logf("rand.Read failed (ignored): %v", err)
				}
				cp := make([]byte, txSize)
				copy(cp, buf)
				exec.InjectTx(cp)
				injectedCount.Add(1)
				injectedBytes.Add(uint64(txSize))
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Recorder goroutine: drain BlobEvents and timestamp them.
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

	// Wait for the pump to finish.
	pumpWg.Wait()
	pumpEnd := time.Now()

	// Drain phase: keep listening for ~3× the DA poll cadence past the
	// last injected tx so in-flight uploads have time to land. Stop early
	// if the inclusion height catches up with the block height.
	const drainTimeout = 30 * time.Second
	drainDeadline := time.Now().Add(drainTimeout)
	for time.Now().Before(drainDeadline) {
		// crude liveness check: if no new receipts in a 5s window, stop.
		receiptsMu.Lock()
		nReceipts := len(receipts)
		receiptsMu.Unlock()
		time.Sleep(2 * time.Second)
		receiptsMu.Lock()
		if len(receipts) == nReceipts {
			receiptsMu.Unlock()
			break
		}
		receiptsMu.Unlock()
	}

	// Stop the node and recorder.
	nodeCancel()
	select {
	case err := <-nodeErrCh:
		// context.Canceled is the expected exit reason after nodeCancel().
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

	// Inter-arrival times of BlobEvents — proxy for upload+settle cadence.
	sort.Slice(finalReceipts, func(i, j int) bool {
		return finalReceipts[i].recvAt.Before(finalReceipts[j].recvAt)
	})
	gaps := make([]float64, 0, len(finalReceipts)-1)
	for i := 1; i < len(finalReceipts); i++ {
		gaps = append(gaps, finalReceipts[i].recvAt.Sub(finalReceipts[i-1].recvAt).Seconds())
	}
	gapP50, gapP99 := percentile(gaps, 0.50), percentile(gaps, 0.99)

	// Single, machine-parseable result line. grep "PERF:" in CI output.
	t.Logf("PERF: blocks=%d txs_executed=%d injected_txs=%d injected_mb=%.2f pump_rate_mb_s=%.2f "+
		"da_blobs=%d da_total_mb=%.2f da_throughput_mb_s=%.2f wall_s=%.2f gap_p50_s=%.3f gap_p99_s=%.3f",
		blocksProduced, txsExecuted, injectedCount.Load(), pumpedMB, pumpRateMBps,
		len(finalReceipts), float64(totalDABytes)/(1024*1024), throughputMBps, wallTime.Seconds(),
		gapP50, gapP99)

	// Sanity: the system should have moved at least *some* bytes through.
	require.Greater(t, totalDABytes, uint64(0), "no DA bytes received via Fiber")
	require.Greater(t, blocksProduced, uint64(0), "no blocks produced during pump window")

	_ = block.FiberBlobEvent{} // keep block import alive in case the recorder branch is trimmed
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
