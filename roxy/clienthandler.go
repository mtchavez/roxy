package roxy

import (
	"bytes"
	"io"
	"net"
	"sync"
)

type ClientHandler struct {
	sync.Mutex
	Conn   net.Conn
	Buff   *bytes.Buffer
	msgLen int
	done   chan bool
}

func ClientListener(conn net.Conn) {
	handler := &ClientHandler{
		Conn:   conn,
		Buff:   bytes.NewBuffer(make([]byte, 64000)),
		msgLen: 0,
		done:   make(chan bool),
	}
	handler.ClientReader()
}

func (handler *ClientHandler) ClientReader() {
	for {
		err := handler.Read()
		if err != nil {
			handler.Close()
			return
		}
		req := handler.MakeRequest()
		go req.Process()
		<-handler.done
	}
}

// Sets up a request for handler and returns request
func (handler *ClientHandler) MakeRequest() *Request {
	req := &Request{}
	req.handler = handler
	req.background = false
	req.mapReducing = false
	return req
}

// Read will read from client connection into the Buff for the ClientHandler
func (handler *ClientHandler) Read() (err error) {
	err = handler.ReadInLengthBuffer()
	if err != nil {
		return
	}
	var bytesRead, b int = 0, 0
	handler.checkBufferSize(handler.msgLen)
	for {
		if bytesRead >= handler.msgLen {
			break
		}
		b, err = handler.Conn.Read(handler.Buff.Bytes()[4+bytesRead:])
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				continue
			}
			return
		}
		bytesRead += b
	}
	return
}

// Gets the length buffer from the client request
func (handler *ClientHandler) ReadInLengthBuffer() (err error) {
	var b int = 0
	readIn := 0
	handler.msgLen = 0
ReadLen:
	b, err = handler.Conn.Read(handler.Buff.Bytes()[readIn:4])
	if err != nil {
		if err != io.EOF {
			handler.Close()
		}
		if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
			goto ReadLen
		}
		return
	}
	readIn += b
	if readIn < 4 {
		goto ReadLen
	}
	handler.msgLen = ParseMessageLength(handler.Buff.Bytes()[:4])
	return
}

// Writes to client
func (handler *ClientHandler) Write(buffer []byte) {
	handler.Conn.Write(buffer)
}

// Closes client connection and removes from RoxyServer known connections
func (handler *ClientHandler) Close() {
	RoxyServer.Lock()
	handler.Conn.Close()
	delete(RoxyServer.Conns, handler.Conn)
	TotalClients--
	if TotalClients < 0 {
		TotalClients = 0
	}
	RoxyServer.Unlock()
}

// Increases buffer if msglen is bigger than current capacity
func (handler *ClientHandler) checkBufferSize(msglen int) {
	if (msglen + 4) < cap(handler.Buff.Bytes()) {
		return
	}
	handler.Buff.Grow(msglen + 10)
}
