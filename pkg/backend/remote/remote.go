package remote

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-engine/pkg/dataconn"
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
	types.ReaderWriterUnmapperAt
	name              string
	replicaServiceURL string
	closeChan         chan struct{}
	monitorChan       types.MonitorChannel
	volumeName        string
}

func (r *Remote) Close() error {
	logrus.Infof("Closing: %s", r.name)
	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure(),
		ptypes.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaClose(ctx, &empty.Empty{}); err != nil {
		return errors.Wrapf(err, "failed to close replica %v from remote", r.replicaServiceURL)
	}

	return nil
}

func (r *Remote) open() error {
	logrus.Infof("Opening remote: %s", r.name)
	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure(),
		ptypes.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaOpen(ctx, &empty.Empty{}); err != nil {
		return errors.Wrapf(err, "failed to open replica %v from remote", r.replicaServiceURL)
	}

	return nil
}

func (r *Remote) Snapshot(name string, userCreated bool, created string, labels map[string]string) error {
	logrus.Infof("Starting to snapshot: %s %s UserCreated %v Created at %v, Labels %v",
		r.name, name, userCreated, created, labels)
	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure(),
		ptypes.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaSnapshot(ctx, &ptypes.ReplicaSnapshotRequest{
		Name:        name,
		UserCreated: userCreated,
		Created:     created,
		Labels:      labels,
	}); err != nil {
		return errors.Wrapf(err, "failed to snapshot replica %v from remote", r.replicaServiceURL)
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

	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure(),
		ptypes.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
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

	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure(),
		ptypes.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.RevisionCounterSet(ctx, &ptypes.RevisionCounterSetRequest{
		Counter: counter,
	}); err != nil {
		return errors.Wrapf(err, "failed to set revision counter to %d for replica %v from remote", counter, r.replicaServiceURL)
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

func (r *Remote) GetUnmapMarkSnapChainRemoved() (bool, error) {
	replicaInfo, err := r.info()
	if err != nil {
		return false, err
	}

	return replicaInfo.UnmapMarkDiskChainRemoved, nil
}

func (r *Remote) SetUnmapMarkSnapChainRemoved(enabled bool) error {
	logrus.Infof("Setting UnmapMarkSnapChainRemoved of %s to : %v", r.name, enabled)

	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure(),
		ptypes.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", r.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.UnmapMarkDiskChainRemovedSet(ctx, &ptypes.UnmapMarkDiskChainRemovedSetRequest{
		Enabled: enabled,
	}); err != nil {
		return errors.Wrapf(err, "failed to set UnmapMarkDiskChainRemoved to %v for replica %v from remote", enabled, r.replicaServiceURL)
	}

	return nil
}

func (r *Remote) info() (*types.ReplicaInfo, error) {
	conn, err := grpc.Dial(r.replicaServiceURL, grpc.WithInsecure(),
		ptypes.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := ptypes.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	resp, err := replicaServiceClient.ReplicaGet(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get replica %v info from remote", r.replicaServiceURL)
	}

	return replicaClient.GetReplicaInfo(resp.Replica), nil
}

func (rf *Factory) Create(volumeName, address string, dataServerProtocol types.DataServerProtocol, engineToReplicaTimeout time.Duration) (types.Backend, error) {
	logrus.Infof("Connecting to remote: %s (%v)", address, dataServerProtocol)

	controlAddress, dataAddress, _, _, err := util.GetAddresses(volumeName, address, dataServerProtocol)
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
		volumeName:  volumeName,
	}

	replica, err := r.info()
	if err != nil {
		return nil, err
	}

	if replica.State != string(types.ReplicaStateClosed) {
		return nil, fmt.Errorf("replica must be closed, cannot add in state: %s", replica.State)
	}

	conn, err := connect(dataServerProtocol, dataAddress)
	if err != nil {
		return nil, err
	}

	dataConnClient := dataconn.NewClient(conn, engineToReplicaTimeout)
	r.ReaderWriterUnmapperAt = dataConnClient

	if err := r.open(); err != nil {
		return nil, err
	}

	go r.monitorPing(dataConnClient)

	return r, nil
}

func connect(dataServerProtocol types.DataServerProtocol, address string) (net.Conn, error) {
	switch dataServerProtocol {
	case types.DataServerProtocolTCP:
		return net.Dial(string(dataServerProtocol), address)
	case types.DataServerProtocolUNIX:
		unixAddr, err := net.ResolveUnixAddr("unix", address)
		if err != nil {
			return nil, err
		}
		return net.DialUnix("unix", nil, unixAddr)
	default:
		return nil, fmt.Errorf("unsupported protocol: %v", dataServerProtocol)
	}
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
