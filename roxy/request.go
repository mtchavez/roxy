package roxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

type Request struct {
	Conn         net.Conn
	ReadIn       chan []byte
	Quit         chan bool
	SharedBuffer []byte
	LengthBuffer []byte
	bytesRead    int
}

func RequestHandler(conn net.Conn) {
	quit := make(chan bool)
	in := make(chan []byte, 8)
	req := &Request{
		Conn:         conn,
		ReadIn:       in,
		Quit:         quit,
		SharedBuffer: make([]byte, 64000),
		LengthBuffer: make([]byte, 4)}
	go req.Sender()
	go req.Reader()
}

func ParseMessageLength(buffer []byte) int {
	var resplength int32
	resplength_buff := bytes.NewBuffer(buffer[0:4])
	err := binary.Read(resplength_buff, binary.BigEndian, &resplength)
	if err != nil {
		fmt.Println(err)
	}
	return int(resplength)
}

func (req *Request) checkBufferSize(msglen int) {
	if (msglen + 4) < len(req.SharedBuffer) {
		return
	}
	newbuff := make([]byte, (msglen+4)*2)
	copy(newbuff, req.SharedBuffer)
	req.SharedBuffer = newbuff
}

func (req *Request) Read() (buffer []byte, err error) {
	_, err = req.Conn.Read(req.LengthBuffer)
	if err != nil {
		return
	}
	var bytesRead int
	msglen := ParseMessageLength(req.LengthBuffer)
	req.checkBufferSize(msglen)
	for {
		if req.bytesRead >= msglen {
			break
		}
		bytesRead, err = req.Conn.Read(req.SharedBuffer)
		if err != nil {
			// TODO: Log error
			return
		}
		req.bytesRead += bytesRead
		buffer = append(buffer, req.SharedBuffer[:bytesRead]...)
	}
	buffer = append(req.LengthBuffer, buffer...)
	req.bytesRead = 0
	return
}

func (req *Request) Write(buffer []byte) {
	req.Conn.Write(buffer)
}

func (req *Request) Close() {
	req.Conn.Close()
	req.Quit <- true
}

func (req *Request) HandleIncoming(incomming []byte) {
	rconn := GetRiakConn()
	_, err := rconn.Conn.Write(incomming)
	if err != nil {
		// TODO: Log error
		return
	}
	_, err = rconn.Conn.Read(req.LengthBuffer)
	msglen := ParseMessageLength(req.LengthBuffer)
	req.checkBufferSize(msglen)
	_, err = rconn.Conn.Read(req.SharedBuffer)
	rconn.Release()
	req.Write(append(req.LengthBuffer, req.SharedBuffer[:msglen]...))
}

func (req *Request) Reader() {
	for {
		buf, err := req.Read()
		if err != nil {
			req.Close()
			break
		}
		req.ReadIn <- buf
	}
}

func (req *Request) Sender() {
	for {
		select {
		case incomming := <-req.ReadIn:
			req.HandleIncoming(incomming)
		case <-req.Quit:
			break
		}
	}
}
