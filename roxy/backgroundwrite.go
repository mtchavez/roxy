package roxy

import (
	"bytes"
	"sync"
)

// Container for keeping total and threshold of background writes
type BackgroundWrites struct {
	total     int
	threshold int
	m         *sync.Mutex
}

var BgWrites = &BackgroundWrites{total: 0, threshold: 1000, m: &sync.Mutex{}}

// Returns true/false if write can be processed in background
func (bg *BackgroundWrites) canProcess() bool {
	return bg.total < bg.threshold
}

// Increment total of background writes
func (bg *BackgroundWrites) incrTotal() {
	bg.m.Lock()
	bg.total++
	bg.m.Unlock()
}

// Decrement total of background writes
func (bg *BackgroundWrites) decrTotal() {
	bg.m.Lock()
	bg.total--
	bg.m.Unlock()
}

// Write the RpbPutReq to riak, called in a Go routine from request.go
func (bg *BackgroundWrites) sendPut(put *bytes.Buffer, msglen int) {
	bg.incrTotal()
	req := &Request{SharedBuffer: put, msgLen: msglen, responded: true}
	req.HandleIncoming()
	bg.decrTotal()
}
