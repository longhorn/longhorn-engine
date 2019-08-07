package engine

import (
	"sync"

	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-instance-manager/rpc"
)

type ProcessStreamWrapper struct {
	grpc.ServerStream
	*sync.RWMutex

	updateChs map[chan<- *rpc.ProcessResponse]struct{}
}

func NewProcessStreamWrapper() *ProcessStreamWrapper {
	return &ProcessStreamWrapper{
		RWMutex:   &sync.RWMutex{},
		updateChs: make(map[chan<- *rpc.ProcessResponse]struct{}),
	}
}

func (sw ProcessStreamWrapper) Send(response *rpc.ProcessResponse) error {
	sw.RLock()
	for ch := range sw.updateChs {
		ch <- response
	}
	sw.RUnlock()
	return nil
}

func (sw *ProcessStreamWrapper) AddLauncherStream(updateCh chan<- *rpc.ProcessResponse) {
	sw.Lock()
	sw.updateChs[updateCh] = struct{}{}
	sw.Unlock()
}

func (sw *ProcessStreamWrapper) RemoveLauncherStream(updateCh chan<- *rpc.ProcessResponse) {
	sw.Lock()
	delete(sw.updateChs, updateCh)
	sw.Unlock()
	close(updateCh)
}
