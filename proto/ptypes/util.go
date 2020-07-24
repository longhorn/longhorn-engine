package ptypes

import (
	"github.com/longhorn/longhorn-engine/pkg/types"
)

func ReplicaModeToGRPCReplicaMode(mode types.Mode) ReplicaMode {
	switch mode {
	case types.WO:
		return ReplicaMode_WO
	case types.RW:
		return ReplicaMode_RW
	case types.ERR:
		return ReplicaMode_ERR
	}
	return ReplicaMode_ERR
}
