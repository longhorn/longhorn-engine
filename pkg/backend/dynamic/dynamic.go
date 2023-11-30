package dynamic

import (
	"fmt"
	"strings"
	"time"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

type Factory struct {
	factories map[string]types.BackendFactory
}

func New(factories map[string]types.BackendFactory) types.BackendFactory {
	return &Factory{
		factories: factories,
	}
}

func (d *Factory) Create(volumeName, address string, dataServerProtocol types.DataServerProtocol, engineToReplicaTimeout time.Duration, nbdEnabled int) (types.Backend, error) {
	parts := strings.SplitN(address, "://", 2)

	if len(parts) == 2 {
		if factory, ok := d.factories[parts[0]]; ok {
			return factory.Create(volumeName, parts[1], dataServerProtocol, engineToReplicaTimeout, nbdEnabled)
		}
	}

	return nil, fmt.Errorf("failed to find factory for %s", address)
}
