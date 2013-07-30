package roxy

import (
	"sync"
)

// Container for keeping total and threshold of background writes
type BackgroundHandler struct {
	sync.Mutex
	total     int
	threshold int
	index     int
	Request   chan *Request
}

var BG_THRESHOLD int
var BgHandler *BackgroundHandler

// Returns true/false if write can be processed in background
func (bg *BackgroundHandler) canProcess() bool {
	bg.Lock()
	processable := bg.total < bg.threshold
	bg.Unlock()
	return processable
}

// Increment total of background writes
func (bg *BackgroundHandler) incrTotal() {
	bg.Lock()
	bg.total++
	bg.Unlock()
}

// Decrement total of background writes
func (bg *BackgroundHandler) decrTotal() {
	bg.Lock()
	bg.total--
	bg.Unlock()
}

func (bg *BackgroundHandler) GetTotal() int {
	bg.Lock()
	t := bg.total
	bg.Unlock()
	return t
}

func (bg *BackgroundHandler) handleInBg(req *Request) {
	bg.incrTotal()
	req.HandleIncoming()
	bg.decrTotal()
}
