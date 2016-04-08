// +build tcmu

package app

import (
	"github.com/rancher/longhorn/frontend/tcmu"
)

func init() {
	frontends["tcmu"] = tcmu.New()
}
