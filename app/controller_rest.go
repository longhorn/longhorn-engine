package app

import (
	"github.com/rancher/longhorn-engine/frontend/rest"
)

func init() {
	frontends["rest"] = rest.New()
}
