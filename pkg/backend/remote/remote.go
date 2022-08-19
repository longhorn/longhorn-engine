package remote

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-engine/pkg/dataconn"
	"github.com/longhorn/longhorn-engine/pkg/replica/client"
	replicaClient "github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
	"github.com/longhorn/longhorn-engine/proto/ptypes"
)

const (
	PingInterval = 2 * time.Second
)

func New() types.BackendFactory {
	return &Factory{}
}

type RevisionCounter struct {
	Counter int64 `json:"counter,string"`
}

type Factory struct {
}

type Remote struct {
	types.ReaderWriterAt
	name              string
	replicaServiceURL string
	closeChan         chan struct{}
	monitorChan       types.MonitorChannel
}

func (r *Remote) Close() error {
	logrus.Infof("Closing: %s", r.name)
	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", r.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), client.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaClose(ctx, &empty.Empty{}); err != nil {
		return fmt.Errorf("failed to close replica %v from remote: %v", r.replicaServiceURL, err)
	}

	return nil
}

func (r *Remote) open() error {
	logrus.Infof("Opening: %s", r.name)
	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", r.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), client.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaOpen(ctx, &empty.Empty{}); err != nil {
		return fmt.Errorf("failed to open replica %v from remote: %v", r.replicaServiceURL, err)
	}

	return nil
}

func (r *Remote) Snapshot(name string, userCreated bool, created string, labels map[string]string) error {
	logrus.Infof("Starting to snapshot: %s %s UserCreated %v Created at %v, Labels %v",
		r.name, name, userCreated, created, labels)
	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", r.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), client.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaSnapshot(ctx, &ptypes.ReplicaSnapshotRequest{
		Name:        name,
		UserCreated: userCreated,
		Created:     created,
		Labels:      labels,
	}); err != nil {
		return fmt.Errorf("failed to snapshot replica %v from remote: %v", r.replicaServiceURL, err)
	}
	logrus.Infof("Finished to snapshot: %s %s UserCreated %v Created at %v, Labels %v",
		r.name, name, userCreated, created, labels)
	return nil
}

func (r *Remote) Expand(size int64) (err error) {
	logrus.Infof("Expand to size %v", size)
	defer func() {
		err = types.WrapError(err, "failed to expand replica %v from remote", r.replicaServiceURL)
	}()

	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", r.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), client.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaExpand(ctx, &ptypes.ReplicaExpandRequest{
		Size: size,
	}); err != nil {
		return types.UnmarshalGRPCError(err)
	}

	return nil
}

func (r *Remote) SetRevisionCounter(counter int64) error {
	logrus.Infof("Set revision counter of %s to : %v", r.name, counter)

	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", r.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), client.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.RevisionCounterSet(ctx, &ptypes.RevisionCounterSetRequest{
		Counter: counter,
	}); err != nil {
		return fmt.Errorf("failed to set revision counter to %d for replica %v from remote: %v", counter, r.replicaServiceURL, err)
	}

	return nil

}

func (r *Remote) Size() (int64, error) {
	replicaInfo, err := r.info()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(replicaInfo.Size, 10, 0)
}

func (r *Remote) IsRevisionCounterDisabled() (bool, error) {
	replicaInfo, err := r.info()
	if err != nil {
		return false, err
	}

	return replicaInfo.RevisionCounterDisabled, nil
}

func (r *Remote) GetLastModifyTime() (int64, error) {
	replicaInfo, err := r.info()
	if err != nil {
		return 0, err
	}
	return replicaInfo.LastModifyTime, nil
}

func (r *Remote) GetHeadFileSize() (int64, error) {
	replicaInfo, err := r.info()
	if err != nil {
		return 0, err
	}
	return replicaInfo.HeadFileSize, nil
}

func (r *Remote) SectorSize() (int64, error) {
	replicaInfo, err := r.info()
	if err != nil {
		return 0, err
	}
	return replicaInfo.SectorSize, nil
}

func (r *Remote) RemainSnapshots() (int, error) {
	replicaInfo, err := r.info()
	if err != nil {
		return 0, err
	}
	switch replicaInfo.State {
	case "open", "dirty", "rebuilding":
		return replicaInfo.RemainSnapshots, nil
	}
	return 0, fmt.Errorf("invalid state %v for counting snapshots", replicaInfo.State)
}

func (r *Remote) GetRevisionCounter() (int64, error) {
	replicaInfo, err := r.info()
	if err != nil {
		return 0, err
	}
	switch replicaInfo.State {
	case "open", "dirty":
		return replicaInfo.RevisionCounter, nil
	}
	return 0, fmt.Errorf("invalid state %v for getting revision counter", replicaInfo.State)
}

func (r *Remote) GetState() (string, error) {
	replicaInfo, err := r.info()
	if err != nil {
		return "", err
	}

	return replicaInfo.State, nil
}

func (r *Remote) info() (*types.ReplicaInfo, error) {
	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to ReplicaService %v: %v", r.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), client.GRPCServiceCommonTimeout)
	defer cancel()

	resp, err := replicaServiceClient.ReplicaGet(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to get replica %v info from remote: %v", r.replicaServiceURL, err)
	}

	return replicaClient.GetReplicaInfo(resp.Replica), nil
}

func (rf *Factory) Create(address string) (types.Backend, error) {
	logrus.Infof("Connecting to remote: %s", address)

	controlAddress, dataAddress, _, _, err := util.ParseAddresses(address)
	if err != nil {
		return nil, err
	}

	r := &Remote{
		name:              address,
		replicaServiceURL: controlAddress,
		// We don't want sender to wait for receiver, because receiver may
		// has been already notified
		closeChan:   make(chan struct{}, 5),
		monitorChan: make(types.MonitorChannel, 5),
	}

	replica, err := r.info()
	if err != nil {
		return nil, err
	}

	if replica.State != "closed" {
		return nil, fmt.Errorf("replica must be closed, Can not add in state: %s", replica.State)
	}

	conn, err := net.Dial("tcp", dataAddress)
	if err != nil {
		return nil, err
	}

	dataConnClient := dataconn.NewClient(conn)
	r.ReaderWriterAt = dataConnClient

	if err := r.open(); err != nil {
		return nil, err
	}

	go r.monitorPing(dataConnClient)

	return r, nil
}

func (r *Remote) monitorPing(client *dataconn.Client) {
	ticker := time.NewTicker(PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.closeChan:
			r.monitorChan <- nil
			return
		case <-ticker.C:
			if err := client.Ping(); err != nil {
				client.SetError(err)
				r.monitorChan <- err
				return
			}
		}
	}
}

func (r *Remote) GetMonitorChannel() types.MonitorChannel {
	return r.monitorChan
}

func (r *Remote) StopMonitoring() {
	r.closeChan <- struct{}{}
}
