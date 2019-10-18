package engine

import (
	"github.com/longhorn/longhorn-engine/controller/client"

	"github.com/longhorn/longhorn-instance-manager/pkg/instance-manager/rpc"
)

type VolumeClientService interface {
	VolumeFrontendStart(addr, launcherName, frontend string) error
	VolumeFrontendShutdown(addr, launcherName string) error
}

type VolumeClient struct{}

func (ec *VolumeClient) VolumeFrontendStart(addr, launcherName, frontend string) error {
	controllerCli := client.NewControllerClient(addr)

	if err := controllerCli.VolumeFrontendStart(frontend); err != nil {
		return err
	}

	return nil
}

func (ec *VolumeClient) VolumeFrontendShutdown(addr, launcherName string) error {
	controllerCli := client.NewControllerClient(addr)

	if err := controllerCli.VolumeFrontendShutdown(); err != nil {
		return err
	}

	return nil
}

type MockVolumeClient struct {
	em *Manager
}

func (mc *MockVolumeClient) VolumeFrontendStart(addr, launcherName, frontend string) error {
	_, err := mc.em.FrontendStartCallback(nil, &rpc.EngineRequest{
		Name: launcherName,
	})
	return err
}

func (mc *MockVolumeClient) VolumeFrontendShutdown(addr, launcherName string) error {
	_, err := mc.em.FrontendShutdownCallback(nil, &rpc.EngineRequest{
		Name: launcherName,
	})
	return err
}
