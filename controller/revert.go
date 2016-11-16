package controller

import (
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/replica/client"
	"github.com/rancher/longhorn/types"
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
		return fmt.Errorf("Not valid state to revert, rebuilding in process or all replica are in ERR state")
	}

	clients, name, err := c.clientsAndSnapshot(name)
	if err != nil {
		return err
	}

	if err := c.shutdownFrontend(); err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()

	minimalSuccess := false
	for address, client := range clients {
		logrus.Infof("Reverting to snapshot %s on %s", name, address)
		if err := client.Revert(name); err != nil {
			logrus.Errorf("Error on reverting to %s on %s: %v", name, address, err)
			c.setReplicaModeNoLock(address, types.ERR)
		} else {
			minimalSuccess = true
			logrus.Infof("Reverting to snapshot %s on %s successed", name, address)
		}
	}

	if !minimalSuccess {
		return fmt.Errorf("Fail to revert to %v on all replicas", name)
	}

	return c.startFrontend()
}

func (c *Controller) clientsAndSnapshot(name string) (map[string]*client.ReplicaClient, string, error) {
	clients := map[string]*client.ReplicaClient{}

	for _, replica := range c.replicas {
		if replica.Mode == types.WO {
			return nil, "", fmt.Errorf("Cannot revert %s during rebuilding process", replica.Address)
		}
		if replica.Mode != types.RW {
			continue
		}

		if !strings.HasPrefix(replica.Address, "tcp://") {
			return nil, "", fmt.Errorf("Backend %s does not support revert", replica.Address)
		}

		repClient, err := client.NewReplicaClient(replica.Address)
		if err != nil {
			return nil, "", err
		}

		_, err = repClient.GetReplica()
		if err != nil {
			return nil, "", err
		}

		/*
			found := ""
			for _, snapshot := range rep.Chain {
				if snapshot == name {
					found = name
					break
				}
				fullName := "volume-snap-" + name + ".img"
				if snapshot == fullName {
					found = fullName
					break
				}
			}

			if found == "" {
				return nil, "", fmt.Errorf("Failed to find snapshot %s on %s", name, replica)
			}

			name = found
		*/
		clients[replica.Address] = repClient
	}
	name = "volume-snap-" + name + ".img"

	return clients, name, nil
}
