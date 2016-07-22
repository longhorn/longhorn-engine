package app

import (
	"github.com/rancher/longhorn/frontend/fusedev"
)

func init() {
	frontends["fuse"] = fusedev.New()
}
