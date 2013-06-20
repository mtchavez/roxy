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

var BG_THRESHOLD int
var BgHandler *BackgroundHandler

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

func (bg *BackgroundHandler) GetTotal() int {
	bg.m.Lock()
	t := bg.total
	bg.m.Unlock()
	return t
}

func (bg *BackgroundHandler) handleInBg(req *Request) {
	bg.incrTotal()
	req.HandleIncoming()
	bg.decrTotal()
}
