//go:build !linux

package sys

import "errors"

// getMountID fails, as mount IDs are Linux-specific.
func getMountID(string) (string, error) {
	return "", errors.New("mount IDs are only available on Linux")
}
