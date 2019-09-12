package process

import (
	"github.com/sirupsen/logrus"
	"time"

	"github.com/longhorn/longhorn-instance-manager/types"
	"github.com/longhorn/longhorn-instance-manager/util"
)

type HealthChecker interface {
	IsRunning(address string) bool
	WaitForRunning(address, name string) bool
}

type GRPCHealthChecker struct{}

func (c *GRPCHealthChecker) IsRunning(address string) bool {
	return util.GRPCServiceReadinessProbe(address)
}

func (c *GRPCHealthChecker) WaitForRunning(address, name string) bool {
	for i := 0; i < types.WaitCount; i++ {
		if c.IsRunning(address) {
			logrus.Infof("Process %v has started", name)
			return true
		}
		logrus.Infof("wait for gRPC service of process %v to start", name)
		time.Sleep(types.WaitInterval)
	}
	return false
}

type MockHealthChecker struct{}

func (c *MockHealthChecker) IsRunning(address string) bool {
	return true
}

func (c *MockHealthChecker) WaitForRunning(address, name string) bool {
	return true
}
