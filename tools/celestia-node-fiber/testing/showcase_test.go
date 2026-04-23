//go:build fibre

package cnfibertest_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/celestia-node/api/client"

	"github.com/evstack/ev-node/block"
	cnfiber "github.com/evstack/ev-node/tools/celestia-node-fiber"
	cnfibertest "github.com/evstack/ev-node/tools/celestia-node-fiber/testing"
)

const (
	// showcaseBlobs is how many distinct-payload blobs the test pushes
	// through the adapter. Large enough to surface ordering and
	// duplicate-handling bugs, small enough to keep wall time reasonable.
	showcaseBlobs = 10

	// listenEventsTimeout bounds the collection window for N BlobEvents.
	// The async MsgPayForFibre broadcasts serialize on the TxClient
	// mutex, so the dominant cost is block_time_per_tx × N. 60s gives
	// ~6s per blob which is generous for a 50ms-precommit testnode.
	listenEventsTimeout = 60 * time.Second
)

// TestShowcase drives the adapter end-to-end with sequential uploads:
// Listen subscribes first, Upload pushes N distinct blobs one at a
// time, the async MsgPayForFibre settlements commit on-chain, the
// subscription delivers an event per blob, and Download round-trips
// each payload byte-for-byte.
func TestShowcase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	adapter, namespace := newShowcaseAdapter(t, ctx)

	events, err := adapter.Listen(ctx, namespace)
	require.NoError(t, err, "starting Listen subscription")

	payloads := buildDistinctPayloads(showcaseBlobs)

	// Sequential uploads — baseline for the parallel test below to
	// compare block-bundling behavior against.
	expected := make(map[string][]byte, showcaseBlobs)
	ids := make([]block.FiberBlobID, showcaseBlobs)
	for i, payload := range payloads {
		res, err := adapter.Upload(ctx, namespace, payload)
		require.NoError(t, err, "adapter.Upload #%d", i)
		require.NotEmpty(t, res.BlobID, "upload #%d returned empty BlobID", i)
		key := hex.EncodeToString(res.BlobID)
		_, dup := expected[key]
		require.False(t, dup, "adapter.Upload #%d returned a duplicate BlobID %s", i, key)
		expected[key] = payload
		ids[i] = res.BlobID
		t.Logf("upload[%02d] ok: blob_id=%s size=%d", i, key, len(payload))
	}

	seen := collectEvents(t, events, expected, time.After(listenEventsTimeout))
	assertEventsMatchPayloads(t, seen, expected)
	logHeightDistribution(t, seen)
	downloadAndDiff(t, ctx, adapter, ids, expected)
}

// TestShowcaseParallel issues all N uploads concurrently from separate
// goroutines. Verifies the adapter and the chain handle parallel async
// MsgPayForFibre settlements without dropping, duplicating, or
// cross-wiring events. Logs block-height distribution so we can see
// whether multiple blobs bundle into the same block when uploads
// arrive closer together in wall time.
func TestShowcaseParallel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	adapter, namespace := newShowcaseAdapter(t, ctx)

	events, err := adapter.Listen(ctx, namespace)
	require.NoError(t, err, "starting Listen subscription")

	payloads := buildDistinctPayloads(showcaseBlobs)

	// Fire all N Uploads in parallel. Each goroutine records its result
	// into a slot; slices keep the write pattern "idx → slot" without
	// any cross-goroutine coordination.
	ids := make([]block.FiberBlobID, showcaseBlobs)
	errs := make([]error, showcaseBlobs)
	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(showcaseBlobs)
	for i := range payloads {
		go func(idx int) {
			defer wg.Done()
			res, err := adapter.Upload(ctx, namespace, payloads[idx])
			if err != nil {
				errs[idx] = err
				return
			}
			ids[idx] = res.BlobID
		}(i)
	}
	wg.Wait()

	uploadWall := time.Since(start)
	for i, err := range errs {
		require.NoError(t, err, "parallel upload #%d failed", i)
		require.NotEmpty(t, ids[i], "parallel upload #%d returned empty BlobID", i)
	}

	// Build expected once all IDs are in. A duplicate BlobID across
	// goroutines would indicate a collision or a race in the adapter.
	expected := make(map[string][]byte, showcaseBlobs)
	for i, id := range ids {
		key := hex.EncodeToString(id)
		_, dup := expected[key]
		require.False(t, dup, "parallel upload #%d produced duplicate BlobID %s", i, key)
		expected[key] = payloads[i]
		t.Logf("upload[%02d] ok: blob_id=%s size=%d", i, key, len(payloads[i]))
	}
	t.Logf("parallel upload wall time: %s for %d blobs", uploadWall, showcaseBlobs)

	seen := collectEvents(t, events, expected, time.After(listenEventsTimeout))
	assertEventsMatchPayloads(t, seen, expected)
	logHeightDistribution(t, seen)
	downloadAndDiff(t, ctx, adapter, ids, expected)
}

// newShowcaseAdapter boots the single-validator chain + fibre server +
// bridge + adapter used by both the sequential and parallel showcases,
// and returns a namespace the caller can use for Upload/Listen.
func newShowcaseAdapter(t *testing.T, ctx context.Context) (*cnfiber.Adapter, []byte) {
	t.Helper()

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

	return adapter, bytes.Repeat([]byte{0xfe}, 10)
}

// buildDistinctPayloads produces N byte slices of strictly increasing
// length so byte-swapping or off-by-one BlobID reconstruction gets
// caught by the download diff.
func buildDistinctPayloads(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte(fmt.Sprintf(
			"showcase blob %02d — payload=%s",
			i, bytes.Repeat([]byte{'a' + byte(i%26)}, 8+i),
		))
	}
	return out
}

// collectEvents drains the adapter's Listen channel until every BlobID
// in expected has been seen exactly once (or deadline hits). Returns
// the collected events keyed by hex(BlobID).
func collectEvents(
	t *testing.T,
	events <-chan block.FiberBlobEvent,
	expected map[string][]byte,
	deadline <-chan time.Time,
) map[string]block.FiberBlobEvent {
	t.Helper()

	total := len(expected)
	seen := make(map[string]block.FiberBlobEvent, total)
	for len(seen) < total {
		select {
		case ev, ok := <-events:
			require.True(t, ok,
				"Listen channel closed with only %d/%d events", len(seen), total)
			key := hex.EncodeToString(ev.BlobID)
			if _, want := expected[key]; !want {
				t.Logf("listen: ignoring unexpected BlobID %s", key)
				continue
			}
			if prev, dup := seen[key]; dup {
				t.Fatalf("listen: duplicate event for BlobID %s (prev height=%d new height=%d)",
					key, prev.Height, ev.Height)
			}
			seen[key] = ev
			t.Logf("listen[%02d/%02d] ok: blob_id=%s height=%d data_size=%d",
				len(seen), total, key, ev.Height, ev.DataSize)
		case <-deadline:
			missing := make([]string, 0, total-len(seen))
			for k := range expected {
				if _, got := seen[k]; !got {
					missing = append(missing, k)
				}
			}
			t.Fatalf("timed out: got %d/%d events; missing=%v",
				len(seen), total, missing)
		}
	}
	return seen
}

// assertEventsMatchPayloads checks every event's DataSize equals the
// original payload length and its Height is non-zero.
func assertEventsMatchPayloads(
	t *testing.T,
	seen map[string]block.FiberBlobEvent,
	expected map[string][]byte,
) {
	t.Helper()
	for key, ev := range seen {
		require.Greater(t, ev.Height, uint64(0),
			"BlobEvent %s must carry a real block height", key)
		require.Equal(t, uint64(len(expected[key])), ev.DataSize,
			"BlobEvent %s DataSize must match original payload length", key)
	}
}

// logHeightDistribution prints how many BlobEvents landed per block
// height, so the test output shows mempool-bundling behavior.
func logHeightDistribution(t *testing.T, seen map[string]block.FiberBlobEvent) {
	t.Helper()

	perHeight := make(map[uint64]int, len(seen))
	for _, ev := range seen {
		perHeight[ev.Height]++
	}
	heights := make([]uint64, 0, len(perHeight))
	for h := range perHeight {
		heights = append(heights, h)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })

	t.Logf("block height distribution (%d heights total):", len(heights))
	for _, h := range heights {
		t.Logf("  height=%d  blobs=%d", h, perHeight[h])
	}
}

// downloadAndDiff round-trips every blob through adapter.Download and
// checks bytes match their upload counterpart.
func downloadAndDiff(
	t *testing.T,
	ctx context.Context,
	adapter *cnfiber.Adapter,
	ids []block.FiberBlobID,
	expected map[string][]byte,
) {
	t.Helper()
	for i, id := range ids {
		key := hex.EncodeToString(id)
		got, err := adapter.Download(ctx, id)
		require.NoError(t, err, "adapter.Download #%d (%s)", i, key)
		require.Equal(t, expected[key], got,
			"Download #%d (%s) bytes mismatch", i, key)
		t.Logf("download[%02d] ok: blob_id=%s bytes=%d", i, key, len(got))
	}
}
