package util

import (
	"github.com/google/uuid"
)

func RandomID(randomIDLenth int) string {
	id := uuid.New().String()
	return id[:randomIDLenth]
}
