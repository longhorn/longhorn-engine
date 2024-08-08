package remote

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/longhorn/types/pkg/generated/enginerpc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/longhorn-engine/pkg/dataconn"
	"github.com/longhorn/longhorn-engine/pkg/interceptor"
	replicaClient "github.com/longhorn/longhorn-engine/pkg/replica/client"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/longhorn/longhorn-engine/pkg/util"
)

const (
	// PingInterval is the time between one successful ping and the next attempt. It is NOT a timeout. The engine will
	// NOT mark a replica as ERR if it fails to receive a response within PingInterval. See monitorPing for details.
	PingInterval = 2 * time.Second

	NumberOfConnections = 2
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

	// Close the dataconn client to avoid orphaning goroutines.
	if dataconnClient, ok := r.ReaderWriterUnmapperAt.(*dataconn.Client); ok {
		dataconnClient.Close()
	}

	conn, err := grpc.NewClient(r.replicaServiceURL, grpc.WithTransportCredentials(insecure.NewCredentials()),
		interceptor.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := enginerpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaClose(ctx, &emptypb.Empty{}); err != nil {
		return errors.Wrapf(err, "failed to close replica %v from remote", r.replicaServiceURL)
	}

	return nil
}

func (r *Remote) open() error {
	logrus.Infof("Opening remote: %s", r.name)
	conn, err := grpc.NewClient(r.replicaServiceURL, grpc.WithTransportCredentials(insecure.NewCredentials()),
		interceptor.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := enginerpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaOpen(ctx, &emptypb.Empty{}); err != nil {
		return errors.Wrapf(err, "failed to open replica %v from remote", r.replicaServiceURL)
	}

	return nil
}

func (r *Remote) Snapshot(name string, userCreated bool, created string, labels map[string]string) error {
	logrus.Infof("Starting to snapshot: %s %s UserCreated %v Created at %v, Labels %v",
		r.name, name, userCreated, created, labels)
	conn, err := grpc.NewClient(r.replicaServiceURL, grpc.WithTransportCredentials(insecure.NewCredentials()),
		interceptor.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := enginerpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaSnapshot(ctx, &enginerpc.ReplicaSnapshotRequest{
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

	conn, err := grpc.NewClient(r.replicaServiceURL, grpc.WithTransportCredentials(insecure.NewCredentials()),
		interceptor.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := enginerpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaExpand(ctx, &enginerpc.ReplicaExpandRequest{
		Size: size,
	}); err != nil {
		return types.UnmarshalGRPCError(err)
	}

	return nil
}

func (r *Remote) SetRevisionCounter(counter int64) error {
	logrus.Infof("Set revision counter of %s to : %v", r.name, counter)

	conn, err := grpc.NewClient(r.replicaServiceURL, grpc.WithTransportCredentials(insecure.NewCredentials()),
		interceptor.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := enginerpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.RevisionCounterSet(ctx, &enginerpc.RevisionCounterSetRequest{
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

func (r *Remote) IsReplicaRebuilding() (bool, error) {
	replicaInfo, err := r.info()
	if err != nil {
		return false, err
	}

	return replicaInfo.Rebuilding, nil
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

func (r *Remote) GetSnapshotCountAndSizeUsage() (int, int64, error) {
	replicaInfo, err := r.info()
	if err != nil {
		return 0, 0, err
	}
	switch replicaInfo.State {
	case "open", "dirty", "rebuilding":
		return replicaInfo.SnapshotCountUsage, replicaInfo.SnapshotSizeUsage, nil
	}
	return 0, 0, fmt.Errorf("invalid state %v for counting snapshots", replicaInfo.State)
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

	conn, err := grpc.NewClient(r.replicaServiceURL, grpc.WithTransportCredentials(insecure.NewCredentials()),
		interceptor.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "failed connecting to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := enginerpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.UnmapMarkDiskChainRemovedSet(ctx, &enginerpc.UnmapMarkDiskChainRemovedSetRequest{
		Enabled: enabled,
	}); err != nil {
		return errors.Wrapf(err, "failed to set UnmapMarkDiskChainRemoved to %v for replica %v from remote", enabled, r.replicaServiceURL)
	}

	return nil
}

func (r *Remote) ResetRebuild() error {
	isRebuilding, err := r.IsReplicaRebuilding()
	if err != nil {
		return err
	}

	if !isRebuilding {
		return nil
	}

	logrus.Warnf("Resetting %v rebuild", r.name)

	conn, err := grpc.NewClient(r.replicaServiceURL, grpc.WithTransportCredentials(insecure.NewCredentials()),
		interceptor.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "failed connecting to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()

	replicaServiceClient := enginerpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	_, err = replicaServiceClient.RebuildingSet(ctx, &enginerpc.RebuildingSetRequest{
		Rebuilding: false,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to set replica %v rebuild to false", r.replicaServiceURL)
	}

	return nil
}

func (r *Remote) SetSnapshotMaxCount(count int) error {
	logrus.Infof("Setting SnapshotMaxCount of %s to : %d", r.name, count)

	conn, err := grpc.NewClient(r.replicaServiceURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		interceptor.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %s", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := enginerpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.SnapshotMaxCountSet(ctx, &enginerpc.SnapshotMaxCountSetRequest{
		Count: int32(count),
	}); err != nil {
		return errors.Wrapf(err, "failed to set SnapshotMaxCount to %d for replica %s from remote", count, r.replicaServiceURL)
	}

	return nil
}

func (r *Remote) SetSnapshotMaxSize(size int64) error {
	logrus.Infof("Setting SnapshotMaxSize of %s to : %d", r.name, size)

	conn, err := grpc.NewClient(r.replicaServiceURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		interceptor.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return errors.Wrapf(err, "cannot connect to ReplicaService %s", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := enginerpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.SnapshotMaxSizeSet(ctx, &enginerpc.SnapshotMaxSizeSetRequest{
		Size: size,
	}); err != nil {
		return errors.Wrapf(err, "failed to set SnapshotMaxSize to %d for replica %s from remote", size, r.replicaServiceURL)
	}

	return nil
}

func (r *Remote) info() (*types.ReplicaInfo, error) {
	conn, err := grpc.NewClient(r.replicaServiceURL, grpc.WithTransportCredentials(insecure.NewCredentials()),
		interceptor.WithIdentityValidationClientInterceptor(r.volumeName, ""))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot connect to ReplicaService %v", r.replicaServiceURL)
	}
	defer conn.Close()
	replicaServiceClient := enginerpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), replicaClient.GRPCServiceCommonTimeout)
	defer cancel()

	resp, err := replicaServiceClient.ReplicaGet(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get replica %v info from remote", r.replicaServiceURL)
	}

	return replicaClient.GetReplicaInfo(resp.Replica), nil
}

func (rf *Factory) Create(volumeName, address string, dataServerProtocol types.DataServerProtocol,
	sharedTimeouts types.SharedTimeouts) (types.Backend, error) {
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

	var conns []net.Conn
	for i := 0; i < NumberOfConnections; i++ {
		conn, err := connect(dataServerProtocol, dataAddress)
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}

	dataConnClient := dataconn.NewClient(conns, sharedTimeouts)
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

// monitorPing sends a TypePing message and waits for a response. It sends additional TypePing messages PingInterval
// after each success. As of https://github.com/longhorn/longhorn-engine/pull/652, the engine will NOT mark a replica as
// ERR if it fails to receive a response within PingInterval.
//   - If there is I/O in flight, then we rely on engine-replica-timeout to prompt marking the replica ERR in the
//     dataconn client loop.
//   - Even if there is no I/O in flight, if there is a problem with the dataconn connection, then we rely on TCP
//     keepalives (15s + 15s * 9 = 150s) or a closing of the connection from the replica side to prompt marking the
//     replica ERR. In the keepalive case, another Longhorn component (e.g. the engine monitor in longhorn-manager) may
//     detect the problem first.
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
