package replica

import (
	"fmt"
	"os"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/backingfile"
)

const (
	Initial    = State("initial")
	Open       = State("open")
	Closed     = State("closed")
	Dirty      = State("dirty")
	Rebuilding = State("rebuilding")
	Error      = State("error")
)

type State string

type Server struct {
	sync.RWMutex
	r                         *Replica
	dir                       string
	sectorSize                int64
	backing                   *backingfile.BackingFile
	revisionCounterDisabled   bool
	unmapMarkDiskChainRemoved bool
}

func NewServer(dir string, backing *backingfile.BackingFile, sectorSize int64, disableRevCounter, unmapMarkDiskChainRemoved bool) *Server {
	return &Server{
		dir:                       dir,
		backing:                   backing,
		sectorSize:                sectorSize,
		revisionCounterDisabled:   disableRevCounter,
		unmapMarkDiskChainRemoved: unmapMarkDiskChainRemoved,
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
	if state != Initial {
		return nil
	}

	sectorSize := s.getSectorSize()

	logrus.Infof("Creating volume %s, size %d/%d", s.dir, size, sectorSize)
	r, err := New(size, sectorSize, s.dir, s.backing, s.revisionCounterDisabled, s.unmapMarkDiskChainRemoved)
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

	logrus.Infof("Opening volume %s, size %d/%d", s.dir, info.Size, sectorSize)
	r, err := New(info.Size, sectorSize, s.dir, s.backing, s.revisionCounterDisabled, s.unmapMarkDiskChainRemoved)
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

	logrus.Infof("Reloading volume")
	newReplica, err := s.r.Reload()
	if err != nil {
		return err
	}

	oldReplica := s.r
	s.r = newReplica
	oldReplica.Close()
	return nil
}

func (s *Server) Status() (State, Info) {
	if s.r == nil {
		info, err := ReadInfo(s.dir)
		if os.IsNotExist(err) {
			return Initial, Info{}
		} else if err != nil {
			logrus.Errorf("Failed to read info in replica directory %s: %v", s.dir, err)
			return Error, Info{}
		}
		return Closed, info
	}
	info := s.r.Info()
	switch {
	case info.Error != "":
		return Error, info
	case info.Rebuilding:
		return Rebuilding, info
	case info.Dirty:
		return Dirty, info
	default:
		return Open, info
	}
}

func (s *Server) SetRebuilding(rebuilding bool) error {
	s.Lock()
	defer s.Unlock()

	state, _ := s.Status()
	// Must be Open/Dirty to set true or must be Rebuilding to set false
	if (rebuilding && state != Open && state != Dirty) ||
		(!rebuilding && state != Rebuilding) {
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

	logrus.Infof("Reverting to snapshot [%s] on volume at %s", name, created)
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

	logrus.Infof("Prepare removing disk: %s", name)
	return s.r.PrepareRemoveDisk(name)
}

func (s *Server) Delete() error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Infof("Deleting volume")
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

	logrus.Infof("Closing volume")
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
		return 0, fmt.Errorf("volume no longer exist")
	}
	i, err := s.r.WriteAt(buf, offset)
	return i, err
}

func (s *Server) ReadAt(buf []byte, offset int64) (int, error) {
	s.RLock()
	defer s.RUnlock()

	if s.r == nil {
		return 0, fmt.Errorf("volume no longer exist")
	}
	i, err := s.r.ReadAt(buf, offset)
	return i, err
}

func (s *Server) UnmapAt(length uint32, off int64) (int, error) {
	s.RLock()
	defer s.RUnlock()

	if s.r == nil {
		return 0, fmt.Errorf("Volume no longer exist")
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
	if state == Error {
		return fmt.Errorf("ping failure due to %v", info.Error)
	}
	if state != Open && state != Dirty && state != Rebuilding {
		return fmt.Errorf("ping failure: replica state %v", state)
	}
	return nil
}
