package rpc

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	. "gopkg.in/check.v1"

	"github.com/longhorn/types/pkg/generated/enginerpc"

	"github.com/longhorn/longhorn-engine/pkg/replica"
	diskutil "github.com/longhorn/longhorn-engine/pkg/util/disk"
)

type fakeReplicaReloadServer struct {
	enginerpc.UnimplementedReplicaServiceServer

	reloaded atomic.Int32
}

func (s *fakeReplicaReloadServer) ReplicaReload(context.Context, *emptypb.Empty) (*enginerpc.ReplicaReloadResponse, error) {
	s.reloaded.Add(1)
	return &enginerpc.ReplicaReloadResponse{}, nil
}

func startFakeReplicaReloadServer(c *C) (string, *fakeReplicaReloadServer, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, IsNil)

	server := grpc.NewServer()
	fake := &fakeReplicaReloadServer{}
	enginerpc.RegisterReplicaServiceServer(server, fake)
	go func() {
		_ = server.Serve(listener)
	}()

	return listener.Addr().String(), fake, server.Stop
}

func chdirTemp(c *C) (string, func()) {
	dir := c.MkDir()
	wd, err := os.Getwd()
	c.Assert(err, IsNil)
	c.Assert(os.Chdir(dir), IsNil)
	return dir, func() {
		c.Assert(os.Chdir(wd), IsNil)
	}
}

func (s *TestSuite) TestIncrementalRestoreInvalidatesChecksum(c *C) {
	dir, restoreDir := chdirTemp(c)
	defer restoreDir()

	address, fake, stop := startFakeReplicaReloadServer(c)
	defer stop()

	snapshotDiskName := "volume-snap-restore.img"
	deltaFileName := diskutil.GenerateDeltaFileName("restore")

	c.Assert(os.WriteFile(snapshotDiskName, bytes.Repeat([]byte{'a'}, 2*purgeTestBlockSize), 0644), IsNil)
	checksumFile := writeTestChecksumFile(c, dir, snapshotDiskName)

	// The delta file only contains data in the second block, and the first block is a hole.
	delta, err := os.Create(deltaFileName)
	c.Assert(err, IsNil)
	c.Assert(delta.Truncate(2*purgeTestBlockSize), IsNil)
	_, err = delta.WriteAt(bytes.Repeat([]byte{'b'}, purgeTestBlockSize), purgeTestBlockSize)
	c.Assert(err, IsNil)
	c.Assert(delta.Close(), IsNil)

	server := &SyncAgentServer{replicaAddress: address}
	err = server.postIncrementalRestoreOperations(&replica.RestoreStatus{
		ToFileName:       deltaFileName,
		SnapshotDiskName: snapshotDiskName,
	})
	c.Assert(err, IsNil)
	c.Assert(fake.reloaded.Load(), Equals, int32(1))

	assertFileNotExist(c, checksumFile)
	assertFileNotExist(c, filepath.Join(dir, deltaFileName))

	f, err := os.Open(snapshotDiskName)
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(f.Close(), IsNil)
	}()
	assertTestBlocks(c, f.ReadAt, []byte{'a', 'b'})
}

func (s *TestSuite) TestIncrementalFullRestoreInvalidatesChecksum(c *C) {
	dir, restoreDir := chdirTemp(c)
	defer restoreDir()

	address, fake, stop := startFakeReplicaReloadServer(c)
	defer stop()

	snapshotDiskName := "volume-snap-restore.img"
	tmpSnapshotDiskName := diskutil.GenerateSnapTempFileName(snapshotDiskName)

	c.Assert(os.WriteFile(snapshotDiskName, bytes.Repeat([]byte{'a'}, purgeTestBlockSize), 0644), IsNil)
	c.Assert(os.WriteFile(diskutil.GenerateSnapshotDiskMetaName(snapshotDiskName), []byte("{}"), 0644), IsNil)
	checksumFile := writeTestChecksumFile(c, dir, snapshotDiskName)

	c.Assert(os.WriteFile(tmpSnapshotDiskName, bytes.Repeat([]byte{'b'}, purgeTestBlockSize), 0644), IsNil)
	c.Assert(os.WriteFile(diskutil.GenerateSnapshotDiskMetaName(tmpSnapshotDiskName), []byte("{}"), 0644), IsNil)

	server := &SyncAgentServer{replicaAddress: address}
	err := server.extraIncrementalFullRestoreOperations(&replica.RestoreStatus{
		ToFileName:       tmpSnapshotDiskName,
		SnapshotDiskName: snapshotDiskName,
	})
	c.Assert(err, IsNil)
	c.Assert(fake.reloaded.Load(), Equals, int32(1))

	assertFileNotExist(c, checksumFile)
	assertFileNotExist(c, filepath.Join(dir, tmpSnapshotDiskName))

	f, err := os.Open(snapshotDiskName)
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(f.Close(), IsNil)
	}()
	assertTestBlocks(c, f.ReadAt, []byte{'b'})
}
