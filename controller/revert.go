package controller

import (
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/replica/client"
	"github.com/rancher/longhorn/types"
)

func (c *Controller) Revert(name string) error {
	for _, rep := range c.replicas {
		if rep.Mode != types.RW {
			return fmt.Errorf("Replica %s is in mode %s", rep.Address, rep.Mode)
		}
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

	for address, client := range clients {
		logrus.Infof("Reverting to snapshot %s on %s", name, address)
		if err := client.Revert(name); err != nil {
			c.setReplicaModeNoLock(address, types.ERR)
			return err
		}
	}

	return c.startFrontend()
}

func (c *Controller) clientsAndSnapshot(name string) (map[string]*client.ReplicaClient, string, error) {
	clients := map[string]*client.ReplicaClient{}

	for _, replica := range c.replicas {
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
