package da

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/evstack/ev-node/block/internal/common"
	"github.com/evstack/ev-node/block/internal/da/fiber"
	datypes "github.com/evstack/ev-node/pkg/da/types"
)

type (
	FiberClient  = fiber.DA
	BlobID       = fiber.BlobID
	UploadResult = fiber.UploadResult
	BlobEvent    = fiber.BlobEvent
)

type FiberConfig struct {
	Client            FiberClient
	Logger            zerolog.Logger
	DefaultTimeout    time.Duration
	Namespace         string
	DataNamespace     string
	LastKnownDAHeight uint64
}

type fiberDAClient struct {
	fiber             FiberClient
	logger            zerolog.Logger
	defaultTimeout    time.Duration
	namespaceBz       []byte
	dataNamespaceBz   []byte
	lastKnownDAHeight uint64

	// latestObservedHeight tracks the highest DA height seen via Subscribe's
	// blob events. Submit and GetLatestDAHeight read it; Subscribe writes
	// it. With Fibre's async-settle design the Upload call returns before
	// the blob lands on chain, so we cannot report the exact settlement
	// height of a freshly uploaded blob — only the latest height observed
	// so far. Seeded from lastKnownDAHeight at construction so first reads
	// before any subscription event still get a sane (non-zero) value.
	latestObservedHeight atomic.Uint64
}

var _ FullClient = (*fiberDAClient)(nil)

func NewFiberClient(cfg FiberConfig) (FullClient, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("fiber client in config is nil")
	}

	if cfg.DefaultTimeout == 0 {
		cfg.DefaultTimeout = 60 * time.Second
	}

	c := &fiberDAClient{
		fiber:             cfg.Client,
		logger:            cfg.Logger.With().Str("component", "fiber_da_client").Logger(),
		defaultTimeout:    cfg.DefaultTimeout,
		lastKnownDAHeight: cfg.LastKnownDAHeight,
		namespaceBz:       datypes.NamespaceFromString(cfg.Namespace).Bytes(),
		dataNamespaceBz:   datypes.NamespaceFromString(cfg.DataNamespace).Bytes(),
	}
	c.latestObservedHeight.Store(cfg.LastKnownDAHeight)
	return c, nil
}

// observeHeight bumps latestObservedHeight if h is larger. CAS loop so
// multiple Subscribe goroutines (e.g. header + data) can both feed it.
func (c *fiberDAClient) observeHeight(h uint64) {
	for {
		cur := c.latestObservedHeight.Load()
		if h <= cur {
			return
		}
		if c.latestObservedHeight.CompareAndSwap(cur, h) {
			return
		}
	}
}

// currentHeight returns the best estimate of the DA tip height. Used by
// Submit (as ResultSubmit.Height) and GetLatestDAHeight. May be zero
// only if the client was constructed with LastKnownDAHeight=0 AND no
// Subscribe event has fired yet.
func (c *fiberDAClient) currentHeight() uint64 {
	return c.latestObservedHeight.Load()
}

func (c *fiberDAClient) Submit(ctx context.Context, data [][]byte, _ float64, namespace []byte, _ []byte) datypes.ResultSubmit {
	if len(data) == 0 {
		return datypes.ResultSubmit{
			BaseResult: datypes.BaseResult{
				Code:           datypes.StatusSuccess,
				SubmittedCount: 0,
				Timestamp:      time.Now(),
			},
		}
	}

	var blobSize uint64
	for _, b := range data {
		blobSize += uint64(len(b))
	}

	for i, raw := range data {
		if uint64(len(raw)) > common.DefaultMaxBlobSize {
			return datypes.ResultSubmit{
				BaseResult: datypes.BaseResult{
					Code:    datypes.StatusTooBig,
					Message: fmt.Sprintf("blob %d exceeds max size (%d > %d)", i, len(raw), common.DefaultMaxBlobSize),
				},
			}
		}
	}

	flat := flattenBlobs(data)

	result, err := c.fiber.Upload(ctx, namespace[len(namespace)-10:], flat)
	if err != nil {
		code := datypes.StatusError
		switch {
		case errors.Is(err, context.Canceled):
			code = datypes.StatusContextCanceled
		case errors.Is(err, context.DeadlineExceeded):
			code = datypes.StatusContextDeadline
		}

		c.logger.Error().Err(err).Msg("fiber upload failed")

		// Nothing was uploaded — the previous len(data)-1 was wrong and
		// could mask data loss if a future caller relied on partial
		// success accounting.
		return datypes.ResultSubmit{
			BaseResult: datypes.BaseResult{
				Code:           code,
				Message:        fmt.Sprintf("fiber upload failed for blob: %v", err),
				SubmittedCount: 0,
				BlobSize:       blobSize,
				Timestamp:      time.Now(),
			},
		}
	}

	// Best-effort DA-tip height. The blob lands a few blocks later via
	// async PFF settlement; the cache only relies on this height being
	// monotonic and ≤ the actual settlement height, both of which hold
	// because Subscribe feeds latestObservedHeight in order from Listen.
	height := c.currentHeight()
	c.logger.Debug().Int("num_ids", len(data)).Uint64("height", height).Msg("fiber DA submission successful")

	return datypes.ResultSubmit{
		BaseResult: datypes.BaseResult{
			Code:           datypes.StatusSuccess,
			IDs:            [][]byte{result.BlobID},
			SubmittedCount: uint64(len(data)),
			Height:         height,
			BlobSize:       blobSize,
			Timestamp:      time.Now(),
		},
	}
}

func (c *fiberDAClient) Retrieve(ctx context.Context, height uint64, namespace []byte) datypes.ResultRetrieve {
	return c.retrieve(ctx, height, namespace, true)
}

func (c *fiberDAClient) RetrieveBlobs(ctx context.Context, height uint64, namespace []byte) datypes.ResultRetrieve {
	return c.retrieve(ctx, height, namespace, false)
}

func (c *fiberDAClient) retrieve(ctx context.Context, height uint64, namespace []byte, _ bool) datypes.ResultRetrieve {
	listenCtx, listenCancel := context.WithTimeout(ctx, c.defaultTimeout)
	defer listenCancel()

	blobCh, err := c.fiber.Listen(listenCtx, namespace[len(namespace)-10:], height)
	if err != nil {
		return datypes.ResultRetrieve{
			BaseResult: datypes.BaseResult{
				Code:      datypes.StatusError,
				Message:   fmt.Sprintf("fiber listen failed: %v", err),
				Height:    height,
				Timestamp: time.Now(),
			},
		}
	}

	var blobIDs []BlobID
loop:
	for {
		select {
		case <-listenCtx.Done():
			break loop
		case event, ok := <-blobCh:
			if !ok {
				break loop
			}
			c.observeHeight(event.Height)
			if event.Height > height {
				break loop
			}
			blobIDs = append(blobIDs, event.BlobID)
		}
	}

	if len(blobIDs) == 0 {
		return datypes.ResultRetrieve{
			BaseResult: datypes.BaseResult{
				Code:      datypes.StatusNotFound,
				Message:   "no blobs found at height for given namespace",
				Height:    height,
				Timestamp: time.Now(),
			},
		}
	}

	ids := make([]datypes.ID, 0, len(blobIDs))
	data := make([][]byte, 0, len(blobIDs))
	for _, blobID := range blobIDs {
		dlCtx, dlCancel := context.WithTimeout(ctx, c.defaultTimeout)
		blobData, dlErr := c.fiber.Download(dlCtx, blobID)
		dlCancel()
		if dlErr != nil {
			return datypes.ResultRetrieve{
				BaseResult: datypes.BaseResult{
					Code:      datypes.StatusError,
					Message:   fmt.Sprintf("fiber download failed for blob %x: %v", blobID, dlErr),
					Height:    height,
					Timestamp: time.Now(),
				},
			}
		}
		split, splitErr := splitBlobs(blobData)
		if splitErr != nil {
			return datypes.ResultRetrieve{
				BaseResult: datypes.BaseResult{
					Code:      datypes.StatusError,
					Message:   fmt.Sprintf("fiber decode failed for blob %x: %v", blobID, splitErr),
					Height:    height,
					Timestamp: time.Now(),
				},
			}
		}
		for _, b := range split {
			ids = append(ids, blobID)
			data = append(data, b)
		}
	}

	return datypes.ResultRetrieve{
		BaseResult: datypes.BaseResult{
			Code:      datypes.StatusSuccess,
			Height:    height,
			IDs:       ids,
			Timestamp: time.Now(),
		},
		Data: data,
	}
}

func (c *fiberDAClient) Get(ctx context.Context, ids []datypes.ID, _ []byte) ([]datypes.Blob, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	res := make([]datypes.Blob, 0, len(ids))
	for _, id := range ids {
		downloadCtx, cancel := context.WithTimeout(ctx, c.defaultTimeout)
		data, err := c.fiber.Download(downloadCtx, id)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("fiber download failed for blob %x: %w", id, err)
		}
		split, splitErr := splitBlobs(data)
		if splitErr != nil {
			return nil, fmt.Errorf("fiber decode failed for blob %x: %w", id, splitErr)
		}
		res = append(res, split...)
	}

	return res, nil
}

const fiberSubscribeChanSize = 42

func (c *fiberDAClient) Subscribe(ctx context.Context, namespace []byte, _ bool) (<-chan datypes.SubscriptionEvent, error) {
	out := make(chan datypes.SubscriptionEvent, fiberSubscribeChanSize)

	go func() {
		defer close(out)

		// The outer DA Subscribe entry point does not expose a starting
		// height, so start from the live tip (fromHeight=0). A future
		// refactor that plumbs resume-from-height through datypes.DA can
		// thread the value here.
		blobCh, err := c.fiber.Listen(ctx, namespace[len(namespace)-10:], c.lastKnownDAHeight)
		if err != nil {
			c.logger.Error().Err(err).Msg("fiber listen failed")
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-blobCh:
				if !ok {
					return
				}

				// Feed the height tracker before any per-event work, so
				// even events with download/decode failures still bump
				// the observed tip — Submit/GetLatestDAHeight use this
				// purely as an "≥ this much has settled" hint.
				c.observeHeight(event.Height)

				blobData, err := c.fiber.Download(ctx, event.BlobID)
				if err != nil {
					c.logger.Error().Err(err).Bytes("blob_id", event.BlobID).Msg("failed to retrieve blob")
					continue
				}

				split, splitErr := splitBlobs(blobData)
				if splitErr != nil {
					c.logger.Error().Err(splitErr).Bytes("blob_id", event.BlobID).Msg("failed to decode blob")
					continue
				}

				select {
				case out <- datypes.SubscriptionEvent{
					Height:    event.Height,
					Timestamp: time.Now(),
					Blobs:     split,
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

func (c *fiberDAClient) GetLatestDAHeight(context.Context) (uint64, error) {
	// Best-effort tip from observed Listen events. With Fibre disabling
	// p2p in the typical deployment this should rarely be called by the
	// hot path, but the syncer's hint-validation code does call it under
	// some peer-gossip scenarios — returning a usable height beats the
	// previous panic.
	return c.currentHeight(), nil
}

func (c *fiberDAClient) GetProofs(_ context.Context, ids []datypes.ID, _ []byte) ([]datypes.Proof, error) {
	return []datypes.Proof{}, fmt.Errorf("not implemented")
}

func (c *fiberDAClient) Validate(_ context.Context, ids []datypes.ID, proofs []datypes.Proof, _ []byte) ([]bool, error) {
	if len(ids) != len(proofs) {
		return nil, errors.New("number of IDs and proofs must match")
	}
	if len(ids) == 0 {
		return []bool{}, nil
	}

	results := make([]bool, len(ids))

	// not implemented.
	for i := range results {
		results[i] = true
	}

	return results, nil
}

func (c *fiberDAClient) GetHeaderNamespace() []byte { return c.namespaceBz }
func (c *fiberDAClient) GetDataNamespace() []byte   { return c.dataNamespaceBz }

func flattenBlobs(blobs [][]byte) []byte {
	if len(blobs) == 0 {
		return nil
	}

	var total int
	for _, b := range blobs {
		total += 4 + len(b)
	}
	total += 4

	buf := make([]byte, total)
	binary.BigEndian.PutUint32(buf, uint32(len(blobs)))
	off := 4
	for _, b := range blobs {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(b)))
		off += 4
		copy(buf[off:], b)
		off += len(b)
	}
	return buf
}

func splitBlobs(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid blob encoding: header too short")
	}

	count := int(binary.BigEndian.Uint32(data))
	off := 4
	blobs := make([][]byte, 0, count)
	for i := range count {
		if off+4 > len(data) {
			return nil, fmt.Errorf("invalid blob encoding: truncated length at index %d", i)
		}
		size := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		end := off + size
		if end < off || end > len(data) {
			return nil, fmt.Errorf("invalid blob encoding: truncated data at index %d", i)
		}
		blob := make([]byte, size)
		copy(blob, data[off:end])
		off = end
		blobs = append(blobs, blob)
	}
	return blobs, nil
}

// Force Inclusion is disabled for Fiber PoC.
func (c *fiberDAClient) HasForcedInclusionNamespace() bool   { return false }
func (c *fiberDAClient) GetForcedInclusionNamespace() []byte { return nil }
