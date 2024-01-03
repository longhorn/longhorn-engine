package replica

import (
	"fmt"
	"os"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/backingfile"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

type Server struct {
	sync.RWMutex
	r                         *Replica
	dir                       string
	sectorSize                int64
	backing                   *backingfile.BackingFile
	revisionCounterDisabled   bool
	unmapMarkDiskChainRemoved bool
	snapshotMaxCount          int
	snapshotMaxSize           int64
}

func NewServer(dir string, backing *backingfile.BackingFile, sectorSize int64, disableRevCounter, unmapMarkDiskChainRemoved bool, snapshotMaxCount int, snapshotMaxSize int64) *Server {
	return &Server{
		dir:                       dir,
		backing:                   backing,
		sectorSize:                sectorSize,
		revisionCounterDisabled:   disableRevCounter,
		unmapMarkDiskChainRemoved: unmapMarkDiskChainRemoved,
		snapshotMaxCount:          snapshotMaxCount,
		snapshotMaxSize:           snapshotMaxSize,
	}
}

func (s *Server) getSectorSize() int64 {
	if s.backing != nil && s.backing.SectorSize > 0 {
		return s.backing.SectorSize
	}
	return s.sectorSize
}

func (s *Server) Create(size int64) error {
	s.Lock()
	defer s.Unlock()

	state, _ := s.Status()
	if state != types.ReplicaStateInitial {
		return nil
	}

	sectorSize := s.getSectorSize()

	logrus.Infof("Creating replica %s, size %d/%d", s.dir, size, sectorSize)
	r, err := New(size, sectorSize, s.dir, s.backing, s.revisionCounterDisabled, s.unmapMarkDiskChainRemoved, s.snapshotMaxCount, s.snapshotMaxSize)
	if err != nil {
		return err
	}

	return r.Close()
}

func (s *Server) Open() error {
	s.Lock()
	defer s.Unlock()

	if s.r != nil {
		return fmt.Errorf("replica is already open")
	}

	_, info := s.Status()
	sectorSize := s.getSectorSize()

	logrus.Infof("Opening replica: dir %s, size %d, sector size %d", s.dir, info.Size, sectorSize)
	r, err := New(info.Size, sectorSize, s.dir, s.backing, s.revisionCounterDisabled, s.unmapMarkDiskChainRemoved, s.snapshotMaxCount, s.snapshotMaxSize)
	if err != nil {
		return err
	}
	s.r = r
	return nil
}

func (s *Server) Reload() error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Info("Reloading replica")
	newReplica, err := s.r.Reload()
	if err != nil {
		return err
	}

	oldReplica := s.r
	s.r = newReplica
	oldReplica.Close()
	return nil
}

func (s *Server) Status() (types.ReplicaState, Info) {
	if s.r == nil {
		info, err := ReadInfo(s.dir)
		if os.IsNotExist(err) {
			return types.ReplicaStateInitial, Info{}
		}

		replica := Replica{dir: s.dir}
		volumeMetaFileValid, validErr := replica.checkValidVolumeMetaData()
		if validErr != nil {
			logrus.WithError(validErr).Errorf("Failed to check if volume metadata is valid in replica directory %s", s.dir)
			return types.ReplicaStateError, Info{}
		}
		if !volumeMetaFileValid {
			return types.ReplicaStateInitial, Info{}
		}

		if err != nil {
			logrus.WithError(err).Errorf("Failed to read info in replica directory %s", s.dir)
			return types.ReplicaStateError, Info{}
		}
		return types.ReplicaStateClosed, info
	}
	info := s.r.Info()
	switch {
	case info.Error != "":
		return types.ReplicaStateError, info
	case info.Rebuilding:
		return types.ReplicaStateRebuilding, info
	case info.Dirty:
		return types.ReplicaStateDirty, info
	default:
		return types.ReplicaStateOpen, info
	}
}

func (s *Server) SetRebuilding(rebuilding bool) error {
	s.Lock()
	defer s.Unlock()

	state, _ := s.Status()
	// Must be Open/Dirty to set true or must be Rebuilding to set false
	if (rebuilding && state != types.ReplicaStateOpen && state != types.ReplicaStateDirty) ||
		(!rebuilding && state != types.ReplicaStateRebuilding) {
		return fmt.Errorf("cannot set rebuilding=%v from state %s", rebuilding, state)
	}

	return s.r.SetRebuilding(rebuilding)
}

func (s *Server) Replica() *Replica {
	return s.r
}

func (s *Server) Revert(name, created string) error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Infof("Reverting to snapshot [%s] on replica at %s", name, created)
	r, err := s.r.Revert(name, created)
	if err != nil {
		return err
	}

	s.r = r
	return nil
}

func (s *Server) Snapshot(name string, userCreated bool, createdTime string, labels map[string]string) error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Infof("Replica server starts to snapshot [%s] volume, user created %v, created time %v, labels %v",
		name, userCreated, createdTime, labels)
	return s.r.Snapshot(name, userCreated, createdTime, labels)
}

func (s *Server) SetUnmapMarkDiskChainRemoved(enabled bool) {
	s.Lock()
	defer s.Unlock()

	s.unmapMarkDiskChainRemoved = enabled
	if s.r != nil {
		s.r.SetUnmapMarkDiskChainRemoved(enabled)
	}

	return
}

func (s *Server) SetSnapshotMaxCount(count int) {
	s.Lock()
	defer s.Unlock()

	s.snapshotMaxCount = count
	if s.r != nil {
		s.r.SetSnapshotMaxCount(count)
	}
}

func (s *Server) SetSnapshotMaxSize(size int64) {
	s.Lock()
	defer s.Unlock()

	s.snapshotMaxSize = size
	if s.r != nil {
		s.r.SetSnapshotMaxSize(size)
	}
}

func (s *Server) Expand(size int64) error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Infof("Replica server starts to expand to size %v", size)

	return s.r.Expand(size)
}

func (s *Server) RemoveDiffDisk(name string, force bool) error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Infof("Removing disk %s, force %v", name, force)
	return s.r.RemoveDiffDisk(name, force)
}

func (s *Server) ReplaceDisk(target, source string) error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Infof("Replacing disk %v with %v", source, target)
	return s.r.ReplaceDisk(target, source)
}

func (s *Server) MarkDiskAsRemoved(name string) error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Infof("Marking disk %v as removed", name)
	return s.r.MarkDiskAsRemoved(name)
}

func (s *Server) PrepareRemoveDisk(name string) ([]PrepareRemoveAction, error) {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil, nil
	}

	logrus.Infof("Prepare removing disk %s", name)
	return s.r.PrepareRemoveDisk(name)
}

func (s *Server) Delete() error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Info("Deleting replica")
	if err := s.r.Close(); err != nil {
		return err
	}

	err := s.r.Delete()
	s.r = nil
	return err
}

func (s *Server) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Info("Closing replica")
	if err := s.r.Close(); err != nil {
		return err
	}

	s.r = nil
	return nil
}

func (s *Server) WriteAt(buf []byte, offset int64) (int, error) {
	s.RLock()
	defer s.RUnlock()

	if s.r == nil {
		return 0, fmt.Errorf("replica no longer exist")
	}
	i, err := s.r.WriteAt(buf, offset)
	return i, err
}

func (s *Server) ReadAt(buf []byte, offset int64) (int, error) {
	s.RLock()
	defer s.RUnlock()

	if s.r == nil {
		return 0, fmt.Errorf("replica no longer exist")
	}
	i, err := s.r.ReadAt(buf, offset)
	return i, err
}

func (s *Server) UnmapAt(length uint32, off int64) (int, error) {
	s.RLock()
	defer s.RUnlock()

	if s.r == nil {
		return 0, fmt.Errorf("replica no longer exist")
	}
	return s.r.UnmapAt(length, off)
}

func (s *Server) SetRevisionCounter(counter int64) error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}
	return s.r.SetRevisionCounter(counter)
}

func (s *Server) PingResponse() error {
	state, info := s.Status()
	if state == types.ReplicaStateError {
		return fmt.Errorf("ping failure due to %v", info.Error)
	}
	if state != types.ReplicaStateOpen && state != types.ReplicaStateDirty && state != types.ReplicaStateRebuilding {
		return fmt.Errorf("ping failure: replica state %v", state)
	}
	return nil
}
