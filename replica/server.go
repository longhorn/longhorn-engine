package replica

import (
	"fmt"
	"os"
	"sync"

	"github.com/Sirupsen/logrus"
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
	r                 *Replica
	dir               string
	defaultSectorSize int64
	backing           *BackingFile
}

func NewServer(dir string, backing *BackingFile, sectorSize int64) *Server {
	return &Server{
		dir:               dir,
		backing:           backing,
		defaultSectorSize: sectorSize,
	}
}

func (s *Server) getSectorSize() int64 {
	if s.backing != nil && s.backing.SectorSize > 0 {
		return s.backing.SectorSize
	}
	return s.defaultSectorSize
}

func (s *Server) getSize(size int64) int64 {
	if s.backing != nil && s.backing.Size > 0 {
		return s.backing.Size
	}
	return size
}

func (s *Server) Create(size int64) error {
	s.Lock()
	defer s.Unlock()

	state, _ := s.Status()
	if state != Initial {
		return nil
	}

	size = s.getSize(size)
	sectorSize := s.getSectorSize()

	logrus.Infof("Creating volume %s, size %d/%d", s.dir, size, sectorSize)
	r, err := New(size, sectorSize, s.dir, s.backing)
	if err != nil {
		return err
	}

	return r.Close()
}

func (s *Server) Open() error {
	s.Lock()
	defer s.Unlock()

	if s.r != nil {
		return fmt.Errorf("Replica is already open")
	}

	_, info := s.Status()
	size := s.getSize(info.Size)
	sectorSize := s.getSectorSize()

	logrus.Infof("Opening volume %s, size %d/%d", s.dir, size, sectorSize)
	r, err := New(size, sectorSize, s.dir, s.backing)
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
			return Error, Info{}
		}
		return Closed, info
	}
	info := s.r.Info()
	switch {
	case info.Rebuilding:
		return Rebuilding, info
	case info.Dirty:
		return Dirty, info
	default:
		return Open, info
	}
}

func (s *Server) SetRebuilding(rebuilding bool) error {
	state, _ := s.Status()
	// Must be Open/Dirty to set true or must be Rebuilding to set false
	if (rebuilding && state != Open && state != Dirty) ||
		(!rebuilding && state != Rebuilding) {
		return fmt.Errorf("Can not set rebuilding=%v from state %s", rebuilding, state)
	}

	return s.r.SetRebuilding(rebuilding)
}

func (s *Server) Replica() *Replica {
	return s.r
}

func (s *Server) Revert(name string) error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Infof("Reverting to snapshot [%s] on volume", name)
	r, err := s.r.Revert(name)
	if err != nil {
		return err
	}

	s.r = r
	return nil
}

func (s *Server) Snapshot(name string) error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Infof("Snapshotting [%s] volume", name)
	return s.r.Snapshot(name)
}

func (s *Server) RemoveDiffDisk(name string) error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Infof("Removing disk: %s", name)
	return s.r.RemoveDiffDisk(name)
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
		return 0, fmt.Errorf("Volume no longer exist")
	}
	i, err := s.r.WriteAt(buf, offset)
	return i, err
}

func (s *Server) ReadAt(buf []byte, offset int64) (int, error) {
	s.RLock()
	defer s.RUnlock()

	if s.r == nil {
		return 0, fmt.Errorf("Volume no longer exist")
	}
	i, err := s.r.ReadAt(buf, offset)
	return i, err
}

func (s *Server) SetRevisionCounter(counter int64) error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}
	return s.r.SetRevisionCounter(counter)
}
