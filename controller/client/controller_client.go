package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-engine/controller/rest"
	congtrollerrpc "github.com/longhorn/longhorn-engine/controller/rpc"
	"github.com/longhorn/longhorn-engine/meta"
	"github.com/longhorn/longhorn-engine/types"
	"github.com/longhorn/longhorn-engine/util"
)

type ControllerClient struct {
	controller  string
	grpcAddress string
}

const (
	GRPCServiceTimeout = 1 * time.Minute
)

func NewControllerClient(controller string) *ControllerClient {
	if !strings.HasSuffix(controller, "/v1") {
		controller += "/v1"
	}

	grpcAddress, err := util.GetControllerGRPCAddress(controller)
	if err != nil {
		logrus.Errorf("Failed to get gRPC address, %v", err)
	}

	return &ControllerClient{
		controller:  controller,
		grpcAddress: grpcAddress,
	}
}

func GetControllerReplicaInfo(cr *congtrollerrpc.ControllerReplica) *types.ControllerReplicaInfo {
	return &types.ControllerReplicaInfo{
		Address: cr.Address.Address,
		Mode:    types.Mode(cr.Mode.String()),
	}
}

func GetControllerReplica(r *types.ControllerReplicaInfo) *congtrollerrpc.ControllerReplica {
	cr := &congtrollerrpc.ControllerReplica{
		Address: &congtrollerrpc.ReplicaAddress{
			Address: r.Address,
		},
	}

	switch r.Mode {
	case types.WO:
		cr.Mode = congtrollerrpc.ReplicaMode_WO
	case types.RW:
		cr.Mode = congtrollerrpc.ReplicaMode_RW
	case types.ERR:
		cr.Mode = congtrollerrpc.ReplicaMode_ERR
	default:
		return nil
	}

	return cr
}

func (c *ControllerClient) VolumeStart(replicas ...string) error {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeStart(ctx, &congtrollerrpc.VolumeStartRequest{
		ReplicaAddresses: replicas,
	}); err != nil {
		return fmt.Errorf("failed to start volume %v: %v", c.grpcAddress, err)
	}

	return nil
}

func (c *ControllerClient) VolumeSnapshot(name string, labels map[string]string) (string, error) {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return "", fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.VolumeSnapshot(ctx, &congtrollerrpc.VolumeSnapshotRequest{
		Name:   name,
		Labels: labels,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot %v for volume %v: %v", name, c.grpcAddress, err)
	}

	return reply.Name, nil
}

func (c *ControllerClient) VolumeRevert(snapshot string) error {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeRevert(ctx, &congtrollerrpc.VolumeRevertRequest{
		Name: snapshot,
	}); err != nil {
		return fmt.Errorf("failed to revert to snapshot %v for volume %v: %v", snapshot, c.grpcAddress, err)
	}

	return nil
}

func (c *ControllerClient) VolumePrepareRestore(lastRestored string) error {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumePrepareRestore(ctx, &congtrollerrpc.VolumePrepareRestoreRequest{
		LastRestored: lastRestored,
	}); err != nil {
		return fmt.Errorf("failed to prepare restoring for volume %v: %v", c.grpcAddress, err)
	}

	return nil
}

func (c *ControllerClient) VolumeFinishRestore(currentRestored string) error {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.VolumeFinishRestore(ctx, &congtrollerrpc.VolumeFinishRestoreRequest{
		CurrentRestored: currentRestored,
	}); err != nil {
		return fmt.Errorf("failed to finish restoring for volume %v: %v", c.grpcAddress, err)
	}

	return nil
}

func (c *ControllerClient) ReplicaList() ([]*types.ControllerReplicaInfo, error) {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.ReplicaList(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to list replicas for volume %v: %v", c.grpcAddress, err)
	}

	replicas := []*types.ControllerReplicaInfo{}
	for _, cr := range reply.Replicas {
		replicas = append(replicas, GetControllerReplicaInfo(cr))
	}

	return replicas, nil
}

func (c *ControllerClient) ReplicaGet(address string) (*types.ControllerReplicaInfo, error) {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	cr, err := controllerServiceClient.ReplicaGet(ctx, &congtrollerrpc.ReplicaAddress{
		Address: address,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get replica %v for volume %v: %v", address, c.grpcAddress, err)
	}

	return GetControllerReplicaInfo(cr), nil
}

func (c *ControllerClient) ReplicaCreate(address string) (*types.ControllerReplicaInfo, error) {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	cr, err := controllerServiceClient.ReplicaCreate(ctx, &congtrollerrpc.ReplicaAddress{
		Address: address,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create replica %v for volume %v: %v", address, c.grpcAddress, err)
	}

	return GetControllerReplicaInfo(cr), nil
}

func (c *ControllerClient) ReplicaDelete(address string) error {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.ReplicaDelete(ctx, &congtrollerrpc.ReplicaAddress{
		Address: address,
	}); err != nil {
		return fmt.Errorf("failed to delete replica %v for volume %v: %v", address, c.grpcAddress, err)
	}

	return nil
}

func (c *ControllerClient) ReplicaUpdate(replica *types.ControllerReplicaInfo) (*types.ControllerReplicaInfo, error) {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	cr, err := controllerServiceClient.ReplicaUpdate(ctx, GetControllerReplica(replica))
	if err != nil {
		return nil, fmt.Errorf("failed to update replica %v for volume %v: %v", replica.Address, c.grpcAddress, err)
	}

	return GetControllerReplicaInfo(cr), nil
}

func (c *ControllerClient) ReplicaPrepareRebuild(address string) (*types.PrepareRebuildOutput, error) {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.ReplicaPrepareRebuild(ctx, &congtrollerrpc.ReplicaAddress{
		Address: address,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to prepare rebuilding replica %v for volume %v: %v", address, c.grpcAddress, err)
	}

	return &types.PrepareRebuildOutput{
		Disks: reply.Disks,
	}, nil
}

func (c *ControllerClient) ReplicaVerifyRebuild(address string) error {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.ReplicaVerifyRebuild(ctx, &congtrollerrpc.ReplicaAddress{
		Address: address,
	}); err != nil {
		return fmt.Errorf("failed to verify rebuilded replica %v for volume %v: %v", address, c.grpcAddress, err)
	}

	return nil
}

func (c *ControllerClient) JournalList(limit int) error {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	if _, err := controllerServiceClient.JournalList(ctx, &congtrollerrpc.JournalListRequest{
		Limit: int64(limit),
	}); err != nil {
		return fmt.Errorf("failed to list journal for volume %v: %v", c.grpcAddress, err)
	}

	return nil
}

func (c *ControllerClient) GetVolume() (*rest.Volume, error) {
	var volumes rest.VolumeCollection

	err := c.get("/volumes", &volumes)
	if err != nil {
		return nil, err
	}

	if len(volumes.Data) == 0 {
		return nil, errors.New("No volume found")
	}

	return &volumes.Data[0], nil
}

func (c *ControllerClient) VersionDetailGet() (*meta.VersionOutput, error) {
	conn, err := grpc.Dial(c.grpcAddress, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to ControllerService %v: %v", c.grpcAddress, err)
	}
	defer conn.Close()
	controllerServiceClient := congtrollerrpc.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	reply, err := controllerServiceClient.VersionDetailGet(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to get version detail: %v", err)
	}

	return &meta.VersionOutput{
		Version:                 reply.Version.Version,
		GitCommit:               reply.Version.GitCommit,
		BuildDate:               reply.Version.BuildDate,
		CLIAPIVersion:           int(reply.Version.CliAPIVersion),
		CLIAPIMinVersion:        int(reply.Version.CliAPIMinVersion),
		ControllerAPIVersion:    int(reply.Version.ControllerAPIVersion),
		ControllerAPIMinVersion: int(reply.Version.ControllerAPIMinVersion),
		DataFormatVersion:       int(reply.Version.DataFormatVersion),
		DataFormatMinVersion:    int(reply.Version.DataFormatMinVersion),
	}, nil

}

func (c *ControllerClient) post(path string, req, resp interface{}) error {
	return c.do("POST", path, req, resp)
}

func (c *ControllerClient) put(path string, req, resp interface{}) error {
	return c.do("PUT", path, req, resp)
}

func (c *ControllerClient) do(method, path string, req, resp interface{}) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}

	bodyType := "application/json"
	url := path
	if !strings.HasPrefix(url, "http") {
		url = c.controller + path
	}

	logrus.Debugf("%s %s", method, url)
	httpReq, err := http.NewRequest(method, url, bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", bodyType)

	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode >= 300 {
		content, _ := ioutil.ReadAll(httpResp.Body)
		return fmt.Errorf("Bad response: %d %s: %s", httpResp.StatusCode, httpResp.Status, content)
	}

	if resp == nil {
		return nil
	}

	return json.NewDecoder(httpResp.Body).Decode(resp)
}

func (c *ControllerClient) get(path string, obj interface{}) error {
	resp, err := http.Get(c.controller + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return json.NewDecoder(resp.Body).Decode(obj)
}
