package roxy

import (
	"sync"
)

// Container for keeping total and threshold of background writes
type BackgroundHandler struct {
	total     int
	threshold int
	index     int
	m         *sync.Mutex
	Request   chan *Request
}

const BG_THRESHOLD = 500

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

func (bg *BackgroundHandler) handleInBg(req *Request) {
	bg.incrTotal()
	req.HandleIncoming()
	bg.decrTotal()
}
