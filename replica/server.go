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
	r          *Replica
	dir        string
	sectorSize int64
}

func NewServer(dir string, sectorSize int64) *Server {
	return &Server{
		dir:        dir,
		sectorSize: sectorSize,
	}
}

func (s *Server) Create(size int64) error {
	s.Lock()
	defer s.Unlock()

	state, _ := s.Status()
	if state != Initial {
		return nil
	}

	logrus.Infof("Creating volume %s, size %d/%d", s.dir, size, s.sectorSize)
	r, err := New(size, s.sectorSize, s.dir)
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
	size := info.Size

	logrus.Infof("Opening volume %s, size %d/%d", s.dir, size, s.sectorSize)
	r, err := New(size, s.sectorSize, s.dir)
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
	} else {
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
}

func (s *Server) SetRebuilding(rebuilding bool) error {
	state, _ := s.Status()
	// Must be Open/Dirty to set true or must be Rebuilding to set false
	if (rebuilding && state != Open && state != Dirty) ||
		(!rebuilding && state != Rebuilding) {
		return fmt.Errorf("Can not set rebuilding=%b from state %s", rebuilding, state)
	}

	return s.r.SetRebuilding(rebuilding)
}

func (s *Server) Replica() *Replica {
	return s.r
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
	i, err := s.r.WriteAt(buf, offset)
	s.RUnlock()
	return i, err
}

func (s *Server) ReadAt(buf []byte, offset int64) (int, error) {
	s.RLock()
	i, err := s.r.ReadAt(buf, offset)
	s.RUnlock()
	return i, err
}
