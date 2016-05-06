package sync

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/client"
	"github.com/rancher/longhorn/controller/rest"
)

type Task struct {
	client *client.ControllerClient
}

func NewTask(controller string) *Task {
	return &Task{
		client: client.NewControllerClient(controller),
	}
}

func (t *Task) AddReplica(replica string) error {
	volume, err := t.client.GetVolume()
	if err != nil {
		return err
	}

	if volume.ReplicaCount == 0 {
		return t.client.Start(replica)
	}

	if err := t.checkAndResetFailedRebuild(replica); err != nil {
		return err
	}

	logrus.Infof("Adding replica %s in WO mode", replica)
	_, err = t.client.CreateReplica(replica)
	if err != nil {
		return err
	}

	fromClient, toClient, err := t.getTransferClients(replica)
	if err != nil {
		return err
	}

	if err := t.syncFiles(fromClient, toClient); err != nil {
		return err
	}

	if err := t.reloadAndCheck(fromClient, toClient); err != nil {
		return err
	}

	return t.setRw(replica)
}

func (t *Task) checkAndResetFailedRebuild(address string) error {
	client, err := client.NewReplicaClient(address)
	if err != nil {
		return err
	}

	replica, err := client.GetReplica()
	if err != nil {
		return err
	}

	if replica.State == "closed" && replica.Rebuilding {
		if err := client.OpenReplica(); err != nil {
			return err
		}

		if err := client.SetRebuilding(false); err != nil {
			return err
		}

		return client.Close()
	}

	return nil
}

func (t *Task) setRw(replica string) error {
	to, err := t.getToReplica(replica)
	if err != nil {
		return err
	}

	to.Mode = "RW"

	to, err = t.client.UpdateReplica(to)
	if err != nil {
		return err
	}

	if to.Mode != "RW" {
		return fmt.Errorf("Failed to set replica to RW, in mode %s", to.Mode)
	}

	return nil
}

func (t *Task) reloadAndCheck(fromClient *client.ReplicaClient, toClient *client.ReplicaClient) error {
	from, err := fromClient.GetReplica()
	if err != nil {
		return err
	}

	to, err := toClient.ReloadReplica()
	if err != nil {
		return err
	}

	fromChain := from.Chain[1:]
	toChain := to.Chain[1:]

	if !reflect.DeepEqual(fromChain, toChain) {
		return fmt.Errorf("Chains are not equal: %v != %v", fromChain, toChain)
	}

	return toClient.SetRebuilding(false)
}

func (t *Task) syncFiles(fromClient *client.ReplicaClient, toClient *client.ReplicaClient) error {
	from, err := fromClient.GetReplica()
	if err != nil {
		return err
	}

	if err := toClient.SetRebuilding(true); err != nil {
		return err
	}

	to, err := toClient.GetReplica()
	if err != nil {
		return err
	}

	fromHead := ""
	toHead := ""

	for _, i := range from.Chain {
		if strings.Contains(i, "volume-head") {
			if fromHead != "" {
				return fmt.Errorf("More than one head volume found in the from replica %s, %s", fromHead, i)
			}
			fromHead = i
			continue
		}

		if err := t.syncFile(i, "", fromClient, toClient); err != nil {
			return err
		}

		if err := t.syncFile(i+".meta", "", fromClient, toClient); err != nil {
			return err
		}
	}

	for _, i := range to.Chain {
		if strings.Contains(i, "volume-head") {
			if toHead != "" {
				return fmt.Errorf("More than one head volume found in the to replica %s, %s", toHead, i)
			}
			toHead = i
			continue
		}
	}

	if fromHead == "" || toHead == "" {
		return fmt.Errorf("Failed to find both source and destination head volumes, %s, %s", fromHead, toHead)
	}

	if err := t.syncFile(fromHead+".meta", toHead+".meta", fromClient, toClient); err != nil {
		return err
	}

	return nil
}

func (t *Task) syncFile(from, to string, fromClient *client.ReplicaClient, toClient *client.ReplicaClient) error {
	host, port, err := toClient.LaunchReceiver()
	if err != nil {
		return err
	}

	if to == "" {
		to = from
	}

	logrus.Infof("Synchronizing %s to %s@%s:%d", from, to, host, port)
	err = fromClient.SendFile(from, to, host, port)
	if err != nil {
		logrus.Infof("Failed synchronizing %s to %s@%s:%d: %v", from, to, host, port, err)
	} else {
		logrus.Infof("Done synchronizing %s to %s@%s:%d", from, to, host, port)
	}

	return err
}

func (t *Task) getTransferClients(address string) (*client.ReplicaClient, *client.ReplicaClient, error) {
	from, err := t.getFromReplica()
	if err != nil {
		return nil, nil, err
	}
	logrus.Infof("Using replica %s as the source for rebuild ", from.Address)

	fromClient, err := client.NewReplicaClient(from.Address)
	if err != nil {
		return nil, nil, err
	}

	to, err := t.getToReplica(address)
	if err != nil {
		return nil, nil, err
	}
	logrus.Infof("Using replica %s as the target for rebuild ", to.Address)

	toClient, err := client.NewReplicaClient(to.Address)
	if err != nil {
		return nil, nil, err
	}

	return fromClient, toClient, nil
}

func (t *Task) getFromReplica() (rest.Replica, error) {
	replicas, err := t.client.ListReplicas()
	if err != nil {
		return rest.Replica{}, err
	}

	for _, r := range replicas {
		if r.Mode == "RW" {
			return r, nil
		}
	}

	return rest.Replica{}, fmt.Errorf("Failed to find good replica to copy from")
}

func (t *Task) getToReplica(address string) (rest.Replica, error) {
	replicas, err := t.client.ListReplicas()
	if err != nil {
		return rest.Replica{}, err
	}

	for _, r := range replicas {
		if r.Address == address {
			if r.Mode != "WO" {
				return rest.Replica{}, fmt.Errorf("Replica %s is not in mode WO got: %s", address, r.Mode)
			}
			return r, nil
		}
	}

	return rest.Replica{}, fmt.Errorf("Failed to find target replica to copy to")
}
