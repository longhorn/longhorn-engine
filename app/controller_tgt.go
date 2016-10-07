package app

import (
	"github.com/rancher/longhorn/frontend/tgt"
)

func init() {
	frontends["tgt"] = tgt.New()
}
