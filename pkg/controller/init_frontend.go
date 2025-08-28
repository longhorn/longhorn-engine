package controller

import (
	"fmt"
	"time"

	devtypes "github.com/longhorn/go-iscsi-helper/types"
	"github.com/longhorn/longhorn-engine/pkg/frontend/rest"
	"github.com/longhorn/longhorn-engine/pkg/frontend/socket"
	"github.com/longhorn/longhorn-engine/pkg/frontend/tgt"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/sirupsen/logrus"
)

const (
	defaultScsiTimeout       = 60 * time.Second // SCSI device timeout
	defaultIscsiAbortTimeout = 15 * time.Second

	DefaultEngineReplicaTimeout = 8 * time.Second
	minEngineReplicaTimeout     = 8 * time.Second
	maxEngineReplicaTimeout     = 30 * time.Second

	// defaultScsiQueueDepth is 32, which is the typical default value set by the Linux iSCSI initiator for iSCSI-TCP devices.
	defaultScsiQueueDepth   = 32
	additionalBufferTimeout = 30 * time.Second
)

func NewFrontend(frontendType string, iscsiTargetRequestTimeout time.Duration) (types.Frontend, error) {
	switch frontendType {
	case "rest":
		return rest.New(), nil
	case "socket":
		return socket.New(), nil
	case devtypes.FrontendTGTBlockDev:
		return tgt.New(devtypes.FrontendTGTBlockDev, defaultScsiTimeout, defaultIscsiAbortTimeout, iscsiTargetRequestTimeout), nil
	case devtypes.FrontendTGTISCSI:
		return tgt.New(devtypes.FrontendTGTISCSI, defaultScsiTimeout, defaultIscsiAbortTimeout, iscsiTargetRequestTimeout), nil
	default:
		return nil, fmt.Errorf("unsupported frontend type: %v", frontendType)
	}
}

func DetermineEngineReplicaTimeout(timeout time.Duration) time.Duration {
	if timeout < minEngineReplicaTimeout ||
		timeout > maxEngineReplicaTimeout {
		logrus.Warnf("Using default engine-replica timeout %v instead since the given value %v is not allowable", DefaultEngineReplicaTimeout, timeout)
		return DefaultEngineReplicaTimeout
	}
	return timeout
}

func DetermineIscsiTargetRequestTimeout(engineReplicaTimeoutLong time.Duration) time.Duration {
	// https://github.com/longhorn/longhorn/issues/8572#issuecomment-3226825317
	// The formula is derived from:
	// ```
	// queue_depth × (2 × engine-replica-timeout) + additional buffer timeout
	// ```
	// 2 × engine-replica-timeout is engineReplicaTimeoutLong
	// queue_depth is SCSI queue depth, which is 32 by default
	return engineReplicaTimeoutLong*defaultScsiQueueDepth + additionalBufferTimeout
}
