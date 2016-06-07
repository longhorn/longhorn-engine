package app

import (
	"github.com/rancher/longhorn/frontend/rest"
)

func init() {
	frontends["rest"] = rest.New()
}
