package roxy

import (
	"bytes"
	"sync"
)

type BackgroundWrites struct {
	total     int
	threshold int
	m         *sync.Mutex
}

var BgWrites = &BackgroundWrites{total: 0, threshold: 1000, m: &sync.Mutex{}}

func (bg *BackgroundWrites) canProcess() bool {
	return bg.total < bg.threshold
}

func (bg *BackgroundWrites) incrTotal() {
	bg.m.Lock()
	bg.total++
	bg.m.Unlock()
}

func (bg *BackgroundWrites) decrTotal() {
	bg.m.Lock()
	bg.total--
	bg.m.Unlock()
}

func (bg *BackgroundWrites) sendPut(put *bytes.Buffer, msglen int) {
	bg.incrTotal()
	req := &Request{SharedBuffer: put, msgLen: msglen, responded: true}
	req.HandleIncoming()
	bg.decrTotal()
}
