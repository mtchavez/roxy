package roxy

import (
	"bytes"
	"sync"
)

// Container for keeping total and threshold of background writes
type BackgroundHandler struct {
	total     int
	threshold int
	m         *sync.Mutex
	Request   chan *Request
}

const BG_THRESHOLD = 000

var BgHandler = &BackgroundHandler{
	total:     0,
	threshold: BG_THRESHOLD,
	m:         &sync.Mutex{},
}

// Returns true/false if write can be processed in background
func (bg *BackgroundHandler) canProcess() bool {
	bg.m.Lock()
	processable := bg.total < bg.threshold
	bg.m.Unlock()
	return processable
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
func (bg *BackgroundHandler) queueToBg(req *Request) {
	bg.incrTotal()
	req.m.Lock()
	defer req.m.Unlock()

	newBytes := make([]byte, 0)
	newBytes = append(newBytes, req.SharedBuffer.Bytes()[:req.msgLen+4]...)
	origBuf := bytes.NewBuffer(newBytes)
	origMsgLen := req.msgLen

	newReq := &Request{
		SharedBuffer: origBuf,
		msgLen:       origMsgLen,
		background:   true,
		m:            &sync.Mutex{},
	}

	go trackTotalBgProcesses()
	go func() {
		newReq.HandleIncoming()
		bg.decrTotal()
	}()
}
