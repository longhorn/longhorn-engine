package controller

import (
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
)

func (c *Controller) Revert(name string) error {
	doable := false
	rw := false
	wo := false
	for _, rep := range c.replicas {
		//BUG: Need to deal with replica in rebuilding(WO) mode.
		if rep.Mode == types.RW {
			rw = true
		} else if rep.Mode == types.WO {
			wo = true
		}
	}
	if rw && !wo {
		doable = true
	}

	if !doable {
		return fmt.Errorf("not valid state to revert, rebuilding in process or all replica are in ERR state")
	}

	clients, name, err := c.clientsAndSnapshot(name)
	if err != nil {
		return err
	}
	defer func() {
		for _, repClient := range clients {
			_ = repClient.Close()
		}
	}()

	if c.FrontendState() == "up" {
		return fmt.Errorf("volume frontend enabled, aborting snapshot revert")
	}

	c.Lock()
	defer c.Unlock()

	minimalSuccess := false
	now := util.Now()
	for address, rClient := range clients {
		logrus.Infof("Reverting to snapshot %s on %s at %s", name, address, now)
		if err := rClient.Revert(name, now); err != nil {
			logrus.Errorf("Error on reverting to %s on %s: %v", name, address, err)
			c.setReplicaModeNoLock(address, types.ERR)
		} else {
			minimalSuccess = true
			logrus.Infof("Reverting to snapshot %s on %s succeeded", name, address)
		}
	}

	if !minimalSuccess {
		return fmt.Errorf("fail to revert to %v on all replicas", name)
	}

	return nil
}

func (c *Controller) clientsAndSnapshot(name string) (map[string]*client.ReplicaClient, string, error) {
	clients := map[string]*client.ReplicaClient{}
	var repClient *client.ReplicaClient
	var err error

	// close all clients in case of error
	defer func() {
		if err != nil {
			for _, repClient := range clients {
				_ = repClient.Close()
			}
		}
	}()

	for _, replica := range c.replicas {
		if replica.Mode == types.WO {
			return nil, "", fmt.Errorf("cannot revert %s during rebuilding process", replica.Address)
		}
		if replica.Mode != types.RW {
			continue
		}

		if !strings.HasPrefix(replica.Address, "tcp://") {
			return nil, "", fmt.Errorf("backend %s does not support revert", replica.Address)
		}

		repClient, err = client.NewReplicaClient(replica.Address)
		if err != nil {
			return nil, "", err
		}
		clients[replica.Address] = repClient

		_, err = repClient.GetReplica()
		if err != nil {
			return nil, "", err
		}
	}
	name = "volume-snap-" + name + ".img"

	return clients, name, nil
}
