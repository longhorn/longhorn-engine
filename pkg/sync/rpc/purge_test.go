package rpc

import (
	"bytes"
	"context"
	"os"
	"path/filepath"

	. "gopkg.in/check.v1"

	"github.com/longhorn/types/pkg/generated/enginerpc"

	"github.com/longhorn/longhorn-engine/pkg/replica"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

const purgeTestBlockSize = 4096

func writeTestChecksumFile(c *C, dir, diskName string) string {
	checksumFile := filepath.Join(dir, diskutil.GenerateSnapshotDiskChecksumName(diskName))
	err := os.WriteFile(checksumFile, []byte(`{"method":"crc64","checksum":"17f13af45475f134"}`), 0644)
	c.Assert(err, IsNil)
	return checksumFile
}

func assertFileNotExist(c *C, path string) {
	_, err := os.Stat(path)
	c.Assert(os.IsNotExist(err), Equals, true, Commentf("%v should not exist", path))
}

func writeTestBlock(c *C, r *replica.Replica, index int, pattern byte) {
	_, err := r.WriteAt(bytes.Repeat([]byte{pattern}, purgeTestBlockSize), int64(index*purgeTestBlockSize))
	c.Assert(err, IsNil)
}

func assertTestBlocks(c *C, readAt func([]byte, int64) (int, error), patterns []byte) {
	for i, pattern := range patterns {
		buf := make([]byte, purgeTestBlockSize)
		_, err := readAt(buf, int64(i*purgeTestBlockSize))
		c.Assert(err, IsNil)
		c.Assert(bytes.Equal(buf, bytes.Repeat([]byte{pattern}, purgeTestBlockSize)), Equals, true,
			Commentf("block %v is expected to be filled with %q", i, pattern))
	}
}

func prepareRemoveActions(c *C, r *replica.Replica, disk string) []*enginerpc.PrepareRemoveAction {
	err := r.MarkDiskAsRemoved(disk)
	c.Assert(err, IsNil)
	actions, err := r.PrepareRemoveDisk(disk)
	c.Assert(err, IsNil)

	result := []*enginerpc.PrepareRemoveAction{}
	for _, action := range actions {
		result = append(result, &enginerpc.PrepareRemoveAction{
			Action: action.Action,
			Source: action.Source,
			Target: action.Target,
		})
	}
	return result
}

// TestPurgeSnapshotInvalidatesChecksum verifies that the checksum files of the snapshot disks
// changed by snapshot purge are removed. See https://github.com/longhorn/longhorn/issues/14096.
func (s *TestSuite) TestPurgeSnapshotInvalidatesChecksum(c *C) {
	dir := c.MkDir()

	// The disk files are handled with the relative paths in the replica directory
	wd, err := os.Getwd()
	c.Assert(err, IsNil)
	c.Assert(os.Chdir(dir), IsNil)
	defer func() {
		c.Assert(os.Chdir(wd), IsNil)
	}()

	size := int64(4 * purgeTestBlockSize)
	r, err := replica.New(context.Background(), size, 512, dir, nil, true, false, 250, 0, false, false,
		types.ReplicaStateInitial, size, "")
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(r.Close(), IsNil)
	}()

	// Chain: volume-snap-000.img (block 0) <- volume-snap-001.img (block 1) <- volume-head-002.img (block 2)
	writeTestBlock(c, r, 0, 'a')
	c.Assert(r.Snapshot("000", true, util.Now(), nil), IsNil)
	writeTestBlock(c, r, 1, 'b')
	c.Assert(r.Snapshot("001", true, util.Now(), nil), IsNil)
	writeTestBlock(c, r, 2, 'c')

	snap0 := "volume-snap-000.img"
	snap1 := "volume-snap-001.img"
	snap0Checksum := writeTestChecksumFile(c, dir, snap0)
	snap1Checksum := writeTestChecksumFile(c, dir, snap1)

	purgeStatus := &PurgeStatus{}
	rmDisk := func(disk string) error {
		return r.RemoveDiffDisk(disk, false)
	}
	replaceDisk := func(source, target string) error {
		return r.ReplaceDisk(target, source)
	}

	// Purge the snapshot with a snapshot child: coalesce and replace
	actions := prepareRemoveActions(c, r, snap0)
	c.Assert(len(actions), Equals, 2)
	c.Assert(actions[0].Action, Equals, replica.OpCoalesce)
	c.Assert(actions[1].Action, Equals, replica.OpReplace)
	c.Assert(executePrepareRemoveActions(actions, purgeStatus, rmDisk, replaceDisk), IsNil)

	assertFileNotExist(c, filepath.Join(dir, snap0))
	assertFileNotExist(c, snap0Checksum)
	assertFileNotExist(c, snap1Checksum)

	snap1File, err := os.Open(filepath.Join(dir, snap1))
	c.Assert(err, IsNil)
	assertTestBlocks(c, snap1File.ReadAt, []byte{'a', 'b'})
	c.Assert(snap1File.Close(), IsNil)
	assertTestBlocks(c, r.ReadAt, []byte{'a', 'b', 'c', 0})

	// Purge the snapshot with the volume head as the child: prune
	writeTestBlock(c, r, 0, 'd')
	snap1Checksum = writeTestChecksumFile(c, dir, snap1)

	actions = prepareRemoveActions(c, r, snap1)
	c.Assert(len(actions), Equals, 1)
	c.Assert(actions[0].Action, Equals, replica.OpPrune)
	c.Assert(executePrepareRemoveActions(actions, purgeStatus, rmDisk, replaceDisk), IsNil)

	assertFileNotExist(c, snap1Checksum)
	assertTestBlocks(c, r.ReadAt, []byte{'d', 'b', 'c', 0})
}

// TestCoalesceInvalidatesChecksumBeforeChangingDisk verifies that the checksum file is removed
// before sparse.FoldFile is called, so it is not left behind no matter whether or where the
// coalescing fails. Here FoldFile fails before writing anything since the child does not exist.
func (s *TestSuite) TestCoalesceInvalidatesChecksumBeforeChangingDisk(c *C) {
	dir := c.MkDir()

	wd, err := os.Getwd()
	c.Assert(err, IsNil)
	c.Assert(os.Chdir(dir), IsNil)
	defer func() {
		c.Assert(os.Chdir(wd), IsNil)
	}()

	parent := "volume-snap-000.img"
	c.Assert(os.WriteFile(filepath.Join(dir, parent), make([]byte, purgeTestBlockSize), 0644), IsNil)
	parentChecksum := writeTestChecksumFile(c, dir, parent)

	actions := []*enginerpc.PrepareRemoveAction{
		{Action: replica.OpCoalesce, Source: parent, Target: "volume-snap-nonexistent.img"},
	}
	err = executePrepareRemoveActions(actions, &PurgeStatus{},
		func(string) error { return nil }, func(string, string) error { return nil })
	c.Assert(err, NotNil)
	assertFileNotExist(c, parentChecksum)
}
