package roxy

import (
	"bytes"
	"encoding/binary"
	"github.com/mtchavez/go-statsite/statsite"
	"io"
	"log"
	"net"
	"time"
)

type Request struct {
	Conn         net.Conn
	ReadIn       chan []byte
	SharedBuffer []byte
	LengthBuffer []byte
	bytesRead    int
	statsEnabled bool
	StatsClient  *statsite.Client
}

func RequestHandler(conn net.Conn) {
	in := make(chan []byte, 8)
	req := &Request{
		Conn:         conn,
		ReadIn:       in,
		SharedBuffer: make([]byte, 64000),
		LengthBuffer: make([]byte, 4),
	}
	if StatsEnabled {
		client, err := InitStatsite()
		if err == nil {
			req.StatsClient = client
		}
	}
	go req.Sender()
	go req.Reader()
}

func ParseMessageLength(buffer []byte) int {
	var resplength int32
	resplength_buff := bytes.NewBuffer(buffer[0:4])
	err := binary.Read(resplength_buff, binary.BigEndian, &resplength)
	if err != nil {
		log.Println("Error parsing message length: ", err)
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
		if err != io.EOF {
			log.Println("Failed to read: ", err)
		}
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
	close(req.ReadIn)
	req.Conn.Close()
	TotalClients--
}

func (req *Request) HandleIncoming(incomming []byte) {
	rconn := GetRiakConn()
ReWrite:
	_, err := rconn.Conn.Write(incomming)
	if err != nil {
		// log.Println("Error writing to Riak: ", err)
		// req.Write([]byte{0, 0, 0, 0})
		// rconn.Release()
		// return
		rconn.Conn.Close()
		time.Sleep(100)
		rconn.ReDialConn()

		log.Println("Rewrite")
		goto ReWrite
	}
Receive:
	_, err = rconn.Conn.Read(req.LengthBuffer)
	msglen := ParseMessageLength(req.LengthBuffer)
	req.checkBufferSize(msglen)
	_, err = rconn.Conn.Read(req.SharedBuffer)
	newbuffer := append(req.LengthBuffer, req.SharedBuffer[:msglen]...)
	startTime := time.Now()
	req.Write(newbuffer)
	endTime := time.Now()
	go req.trackLatency(startTime, endTime)
	go req.trackCmdsProcessed()
	if newbuffer[4] == commandToNum["RpbMapRedResp"] &&
		!bytes.Equal(newbuffer, MapRedDone) {
		goto Receive
	}
	rconn.Release()
}

func (req *Request) Reader() {
	buf, err := req.Read()
	if err != nil {
		req.Close()
		return
	}
	req.ReadIn <- buf
}

func (req *Request) Sender() {
	for incoming := range req.ReadIn {
		req.HandleIncoming(incoming)
		req.Reader()
	}
}
