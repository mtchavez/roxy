package roxy

import (
	"bytes"
	// "log"
	"runtime"
	"sync"
)

// Container for keeping total and threshold of background writes
type BackgroundHandler struct {
	total     int
	threshold int
	m         *sync.Mutex
	Request   chan *Request
}

const BG_THRESHOLD = 500

var BgHandler = &BackgroundHandler{
	total:     0,
	threshold: BG_THRESHOLD,
	m:         &sync.Mutex{},
	Request:   make(chan *Request, BG_THRESHOLD),
}

// Returns true/false if write can be processed in background
func (bg *BackgroundHandler) canProcess() bool {
	return bg.total < bg.threshold
}

// Increment total of background writes
func (bg *BackgroundHandler) incrTotal() {
	bg.m.Lock()
	bg.total++
	bg.m.Unlock()
}

// Decrement total of background writes
func (bg *BackgroundHandler) decrTotal() {
	bg.m.Lock()
	bg.total--
	bg.m.Unlock()
}

// Write the RpbPutReq to riak, called in a Go routine from request.go
func (bg *BackgroundHandler) queueToBg(put *bytes.Buffer, msglen int) {
	bg.incrTotal()
	newBytes := make([]byte, cap(put.Bytes()))
	copy(put.Bytes()[:], newBytes)
	newBuf := bytes.NewBuffer(newBytes)
	req := &Request{SharedBuffer: newBuf, msgLen: msglen, background: true}
	go func() {
		req.HandleIncoming()
		bg.decrTotal()
		TotalClients--
		runtime.GC()
	}()
}
