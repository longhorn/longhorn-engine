package controller

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

type testset struct {
	volumeSize        int64
	volumeCurrentSize int64
	backendSizes      map[int64]struct{}
	expectedSize      int64
}

func (s *TestSuite) TestDetermineCorrectVolumeSize(c *C) {
	testsets := []testset{
		{
			volumeSize:        64,
			volumeCurrentSize: 0,
			backendSizes: map[int64]struct{}{
				64: {},
			},
			expectedSize: 64,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 0,
			backendSizes: map[int64]struct{}{
				0: {},
			},
			expectedSize: 64,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 64,
			backendSizes: map[int64]struct{}{
				64: {},
			},
			expectedSize: 64,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 64,
			backendSizes: map[int64]struct{}{
				32: {},
			},
			expectedSize: 64,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 32,
			backendSizes: map[int64]struct{}{
				64: {},
			},
			expectedSize: 64,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 32,
			backendSizes: map[int64]struct{}{
				32: {},
			},
			expectedSize: 32,
		},
		{
			volumeSize:        64,
			volumeCurrentSize: 32,
			backendSizes: map[int64]struct{}{
				32: {},
				64: {},
			},
			expectedSize: 32,
		},
	}

	for _, t := range testsets {
		size := determineCorrectVolumeSize(t.volumeSize, t.volumeCurrentSize, t.backendSizes)
		c.Assert(size, Equals, t.expectedSize)
	}
}
