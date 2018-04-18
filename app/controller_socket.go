package app

import (
	"github.com/rancher/longhorn-engine/frontend/socket"
)

func init() {
	frontends["socket"] = socket.New()
}
