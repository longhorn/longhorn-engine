package instance

import (
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"

	"github.com/longhorn/longhorn-instance-manager/rpc"
	"github.com/longhorn/longhorn-instance-manager/util"
)

type Manager struct{}

func NewInstanceManagerServer() *Manager {
	return &Manager{}
}

func (im *Manager) EnvironmentVariableSet(ctx context.Context, req *rpc.EnvironmentVariableSetRequset) (ret *empty.Empty, err error) {
	args := []string{}
	for k, v := range req.Variables {
		args = append(args, fmt.Sprintf("%s=%s", k, v))
	}

	if _, err := util.Execute("export", args...); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}
