package dynamic
//dependancies
import (
	"fmt"
	"strings"

	"github.com/rancher/longhorn-engine/types"
)
//struct created
type Factory struct {
	factories map[string]types.BackendFactory
}

func New(factories map[string]types.BackendFactory) types.BackendFactory {
	return &Factory{
		factories: factories,
	}
}

func (d *Factory) Create(address string) (types.Backend, error) {
	parts := strings.SplitN(address, "://", 2)

	if len(parts) == 2 {
		if factory, ok := d.factories[parts[0]]; ok {
			return factory.Create(parts[1])
		}
	}

	return nil, fmt.Errorf("Failed to find factory for %s", address)
}
