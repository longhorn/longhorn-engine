package backupstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/backupstore/util"

	lhbackup "github.com/longhorn/go-common-libs/backup"
)

func getBlockPath(volumeName string) string {
	return filepath.Join(getVolumePath(volumeName), BLOCKS_DIRECTORY) + "/"
}

func getBlockFilePath(volumeName, checksum string) string {
	blockSubDirLayer1 := checksum[0:BLOCK_SEPARATE_LAYER1]
	blockSubDirLayer2 := checksum[BLOCK_SEPARATE_LAYER1:BLOCK_SEPARATE_LAYER2]
	path := filepath.Join(getBlockPath(volumeName), blockSubDirLayer1, blockSubDirLayer2)
	fileName := checksum + BLK_SUFFIX

	return filepath.Join(path, fileName)
}

// mergeErrorChannels will merge all error channels into a single error out channel.
// the error out channel will be closed once the ctx is done or all error channels are closed
// if there is an error on one of the incoming channels the error will be relayed.
func mergeErrorChannels(ctx context.Context, channels ...<-chan error) <-chan error {
	var wg sync.WaitGroup
	wg.Add(len(channels))

	out := make(chan error, len(channels))
	output := func(c <-chan error) {
		defer wg.Done()
		select {
		case err, ok := <-c:
			if ok {
				out <- err
			}
			return
		case <-ctx.Done():
			return
		}
	}

	for _, c := range channels {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// Retry backoff intervals.
var backoffDuration = [...]time.Duration{
	time.Second,
	5 * time.Second,
	30 * time.Second,
	1 * time.Minute,
	2 * time.Minute,
	5 * time.Minute,
}

func retryWithBackoff(ctx context.Context, f func() (io.Reader, error)) (io.Reader, error) {
	var lastErr error
	for attempt := 0; ; attempt++ {
		r, err := f()
		if err == nil {
			return r, nil
		}
		if strings.Contains(err.Error(), "checksum verification failed") {
			return nil, err
		}
		lastErr = err
		if attempt >= len(backoffDuration) {
			return nil, errors.Wrapf(lastErr, "failed after %d attempts", attempt+1)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoffDuration[attempt]):
		}
	}
}

// DecompressAndVerifyWithFallback reads, decompresses, and verifies a block.
// Retries with backoff on transient read/verify errors and falls back to gzip<->lz4
// when header/magic suggests a mismatched method. Rationale: remote stores can be
// transient; retries avoid spurious failures while checksum preserves integrity.
func DecompressAndVerifyWithFallback(ctx context.Context, bsDriver BackupStoreDriver, blkFile, decompression, checksum string) (io.Reader, error) {
	// Helper function to read block from backup store
	readBlock := func() (io.ReadCloser, error) {
		rc, err := bsDriver.Read(blkFile)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read block %v", blkFile)
		}
		return rc, nil
	}

	return retryWithBackoff(ctx, func() (io.Reader, error) {
		rc, err := readBlock()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read block %v", blkFile)
		}
		buf, readErr := io.ReadAll(rc)
		_ = rc.Close()
		if readErr != nil {
			return nil, errors.Wrapf(readErr, "failed to read block %v into buffer", blkFile)
		}
		r, err := util.DecompressAndVerify(decompression, bytes.NewReader(buf), checksum)
		if err == nil {
			return r, nil
		}
		alternativeDecompression := ""
		if errors.Is(err, gzip.ErrHeader) {
			alternativeDecompression = "lz4"
		} else if strings.Contains(err.Error(), "lz4: bad magic number") {
			alternativeDecompression = "gzip"
		}
		if alternativeDecompression != "" {
			rAlt, errAlt := util.DecompressAndVerify(alternativeDecompression, bytes.NewReader(buf), checksum)
			if errAlt == nil {
				return rAlt, nil
			}
			return nil, errors.Wrapf(errAlt, "fallback decompression also failed for block %v", blkFile)
		}
		return nil, errors.Wrapf(err, "decompression verification failed for block %v", blkFile)
	})
}

func getBlockSizeFromParameters(parameters map[string]string) (int64, error) {
	if parameters == nil {
		return DEFAULT_BLOCK_SIZE, nil
	}
	sizeVal, exist := parameters[lhbackup.LonghornBackupParameterBackupBlockSize]
	if !exist || sizeVal == "" {
		return DEFAULT_BLOCK_SIZE, nil
	}
	quantity, err := resource.ParseQuantity(sizeVal)
	if err != nil {
		return 0, errors.Wrapf(err, "invalid block size %s from parameter %s", sizeVal, lhbackup.LonghornBackupParameterBackupBlockSize)
	}
	if quantity.IsZero() {
		return DEFAULT_BLOCK_SIZE, nil
	}
	return quantity.Value(), nil
}
