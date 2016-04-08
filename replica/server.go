package replica

import (
	"fmt"
	"sync"

	"github.com/Sirupsen/logrus"
)

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

func (s *Server) Open(size int64) error {
	s.Lock()
	defer s.Unlock()

	if s.r != nil {
		return fmt.Errorf("Replica is already open")
	}

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

func (s *Server) Replica() *Replica {
	return s.r
}

func (s *Server) Snapshot() error {
	s.Lock()
	defer s.Unlock()

	if s.r == nil {
		return nil
	}

	logrus.Infof("Snapshotting volume")
	return s.r.Snapshot()
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
