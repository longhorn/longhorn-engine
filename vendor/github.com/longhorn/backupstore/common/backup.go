package common

import (
	"context"
	"sync"

	"github.com/longhorn/backupstore"
)

type ProgressState string

const (
	ProgressStateInProgress = ProgressState("in_progress")
	ProgressStateComplete   = ProgressState("complete")
	ProgressStateError      = ProgressState("error")
)

const (
	ProgressPercentageBackup      = 95
	ProgressPercentageBackupTotal = 100
)

type Mapping struct {
	Offset int64
	Size   int64
}

type Mappings struct {
	Mappings  []Mapping
	BlockSize int64
}

type MessageType string

const (
	MessageTypeError = MessageType("error")
)

type BlockMapping struct {
	Offset        int64
	BlockChecksum string
}

type BlockInfo struct {
	Checksum string
	Path     string
	Refcount int
}

type Block struct {
	Offset            int64
	BlockChecksum     string
	CompressionMethod string
	IsZeroBlock       bool
}

type ProcessingBlocks struct {
	sync.Mutex
	Blocks map[string][]*BlockMapping
}

type Progress struct {
	sync.Mutex

	TotalBlockCounts     int64
	ProcessedBlockCounts int64
	NewBlockCounts       int64

	Progress int
}

func PopulateMappings(bsDriver backupstore.BackupStoreDriver, mappings *Mappings) (<-chan Mapping, <-chan error) {
	mappingChan := make(chan Mapping, 1)
	errChan := make(chan error, 1)

	go func() {
		defer close(mappingChan)
		defer close(errChan)

		for _, mapping := range mappings.Mappings {
			mappingChan <- mapping
		}
	}()

	return mappingChan, errChan
}

func PopulateBlocksForFullRestore(blocks []BlockMapping, compressionMethod string) (<-chan *Block, <-chan error) {
	blockChan := make(chan *Block, 10)
	errChan := make(chan error, 1)

	go func() {
		defer close(blockChan)
		defer close(errChan)

		for _, block := range blocks {
			blockChan <- &Block{
				Offset:            block.Offset,
				BlockChecksum:     block.BlockChecksum,
				CompressionMethod: compressionMethod,
			}
		}
	}()

	return blockChan, errChan
}

// MergeErrorChannels merges the error channels into a single output channel.
//
// Each input channel is expected to carry the result of one worker: the worker sends at most
// one error and then closes the channel. Each error received is passed on to the output.
//
// If ctx is cancelled before an input delivers a value (even if the input closes without
// sending), ctx.Err() is passed on instead. This way the caller sees cancellation as a
// failure, not as a clean completion.
//
// The output is closed once every input has been handled: it delivered a value, it closed,
// or the cancelled ctx cut it off.
func MergeErrorChannels(ctx context.Context, channels ...<-chan error) <-chan error {
	var wg sync.WaitGroup
	wg.Add(len(channels))

	// Buffered to len(channels) and each goroutine sends at most once, so no send blocks even
	// after the caller stops reading.
	out := make(chan error, len(channels))
	output := func(c <-chan error) {
		defer wg.Done()
		select {
		case err, ok := <-c:
			if ok {
				out <- err
			} else if ctxErr := ctx.Err(); ctxErr != nil {
				// The input closed while ctx was already cancelled. Both select cases were
				// ready, and this receive case can win over ctx.Done(); send the cancellation
				// so the caller does not read a closed output as success.
				out <- ctxErr
			}
		case <-ctx.Done():
			out <- ctx.Err()
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

// GetProgress returns the progress percentage to report after processedBlocks of totalBlocks
// have been backed up or restored.
//
// processedBlocks is a count that includes the block that just finished, not a zero-based index,
// so the last block yields exactly ProgressPercentageBackup.
//
// Values above ProgressPercentageBackup up to ProgressPercentageBackupTotal are reserved for the
// final status update, which runs after the metadata is saved or the backing image file is closed.
// Per-block progress must never report completion.
func GetProgress(totalBlocks, processedBlocks int64) int {
	return int((float64(processedBlocks) / float64(totalBlocks)) * ProgressPercentageBackup)
}

func SortBackupBlocks(blocks []BlockMapping, size, blockSize int64) []BlockMapping {
	blocksNum := size / blockSize
	if size%blockSize > 0 {
		blocksNum++
	}
	sortedBlocks := make([]string, blocksNum)
	for _, block := range blocks {
		i := block.Offset / blockSize
		sortedBlocks[i] = block.BlockChecksum
	}

	blockMappings := []BlockMapping{}
	for i, checksum := range sortedBlocks {
		if checksum != "" {
			blockMappings = append(blockMappings, BlockMapping{
				Offset:        int64(i) * blockSize,
				BlockChecksum: checksum,
			})
		}
	}

	return blockMappings
}

func UpdateBlockReferenceCount(blockInfos map[string]*BlockInfo, blocks []BlockMapping, driver backupstore.BackupStoreDriver) {
	for _, block := range blocks {
		info, known := blockInfos[block.BlockChecksum]
		if !known {
			info = &BlockInfo{Checksum: block.BlockChecksum}
			blockInfos[block.BlockChecksum] = info
		}
		info.Refcount++
	}
}

func IsBlockSafeToDelete(blk *BlockInfo) bool {
	return isBlockPresent(blk) && !isBlockReferenced(blk)
}

func isBlockPresent(blk *BlockInfo) bool {
	return blk != nil && blk.Path != ""
}

func isBlockReferenced(blk *BlockInfo) bool {
	return blk != nil && blk.Refcount > 0
}
