package engine

import (
	"sync"

	"github.com/longhorn/go-iscsi-helper/longhorndev"
)

type MockDeviceCreator struct{}

func (mdc *MockDeviceCreator) NewDevice(name string, size int64, frontend string) (longhorndev.DeviceService, error) {
	return &MockDeviceService{
		RWMutex:  &sync.RWMutex{},
		name:     name,
		frontend: frontend,
		size:     size,
	}, nil
}

type MockDeviceService struct {
	*sync.RWMutex
	name     string
	frontend string
	endpoint string
	enabled  bool
	size     int64
	tID      int
}

func (mds *MockDeviceService) GetFrontend() string {
	mds.RLock()
	defer mds.RUnlock()
	return mds.frontend
}

func (mds *MockDeviceService) SetFrontend(frontend string) error {
	mds.Lock()
	defer mds.Unlock()
	mds.frontend = frontend
	return nil
}

func (mds *MockDeviceService) UnsetFrontendCheck() error {
	mds.RLock()
	defer mds.RUnlock()
	return nil
}

func (mds *MockDeviceService) UnsetFrontend() {
	mds.Lock()
	defer mds.Unlock()
	mds.frontend = ""
}

func (mds *MockDeviceService) GetEndpoint() string {
	mds.RLock()
	defer mds.RUnlock()
	return mds.endpoint
}

func (mds *MockDeviceService) Enabled() bool {
	mds.RLock()
	defer mds.RUnlock()
	return mds.enabled
}

func (mds *MockDeviceService) Start(tID int) error {
	mds.Lock()
	defer mds.Unlock()
	mds.enabled = true
	mds.tID = tID
	return nil
}

func (mds *MockDeviceService) Shutdown() (int, error) {
	mds.Lock()
	defer mds.Unlock()
	mds.enabled = false
	oldTID := mds.tID
	mds.tID = 0
	return oldTID, nil
}

func (mds *MockDeviceService) PrepareUpgrade() error {
	return nil
}
func (mds *MockDeviceService) FinishUpgrade() error {
	return nil
}
