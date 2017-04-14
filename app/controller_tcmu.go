// +build tcmu

package app

import (
	"github.com/rancher/longhorn-engine/frontend/tcmu"
)

func init() {
	frontends["tcmu"] = tcmu.New()
}
