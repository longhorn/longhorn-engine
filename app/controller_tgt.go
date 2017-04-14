package app

import (
	"github.com/rancher/longhorn-engine/frontend/tgt"
)

func init() {
	frontends["tgt"] = tgt.New()
}
