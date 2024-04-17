package dataconn

import (
	"sync"
)

type MultiClient struct {
	lock    sync.Mutex
	clients []*Client
	next    int
}

func NewMultiClient(clients []*Client) *MultiClient {
	mc := &MultiClient{
		clients: clients,
	}
	return mc
}

func (mc *MultiClient) getNextClient() *Client {
	mc.lock.Lock()
	mc.next = (mc.next + 1) % len(mc.clients)
	index := mc.next
	mc.lock.Unlock()
	return mc.clients[index]
}

func (mc *MultiClient) ReadAt(buf []byte, offset int64) (int, error) {
	return mc.getNextClient().ReadAt(buf, offset)
}

func (mc *MultiClient) WriteAt(buf []byte, offset int64) (int, error) {
	return mc.getNextClient().WriteAt(buf, offset)
}

func (mc *MultiClient) UnmapAt(length uint32, offset int64) (int, error) {
	return mc.getNextClient().UnmapAt(length, offset)
}
