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
	ReadIn       chan bool
	SharedBuffer *bytes.Buffer
	msgLen       int
	statsEnabled bool
	StatsClient  *statsite.Client
}

func RequestHandler(conn net.Conn) {
	in := make(chan bool, 8)
	req := &Request{
		Conn:         conn,
		ReadIn:       in,
		SharedBuffer: bytes.NewBuffer(make([]byte, 64000)),
		msgLen:       0,
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
	if (msglen + 4) < req.SharedBuffer.Len() {
		return
	}
	req.SharedBuffer.Grow(msglen + 4)
}

func (req *Request) Read() (err error) {
	var readIn int = 0
	var b int = 0
	req.msgLen = 0
ReadLen:
	b, err = req.Conn.Read(req.SharedBuffer.Bytes()[readIn:4])
	if err != nil {
		if err != io.EOF {
			// log.Println("Failed to read: ", err)
			req.Close()
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
	// log.Println("BytesRead: ", req.SharedBuffer.Bytes()[:4])
	var bytesRead int = 0
	b = 0
	req.msgLen = ParseMessageLength(req.SharedBuffer.Bytes()[:4])
	req.checkBufferSize(req.msgLen)
	for {
		if bytesRead >= req.msgLen {
			break
		}
		b, err = req.Conn.Read(req.SharedBuffer.Bytes()[4+bytesRead:])
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				continue
			}
			req.Close()
			return
		}
		bytesRead += b
	}
	// log.Println("Read: ", numToCommand[int(req.SharedBuffer.Bytes()[4])])
	// log.Println("MSGLEN = ", req.msgLen)
	return
}

func (req *Request) Write(buffer []byte) {
	// log.Println("WRITING TO CLIENT: ", buffer)
	req.Conn.Write(buffer)
}

func (req *Request) Close() {
	// _, ok := <-req.ReadIn
	// if ok {
	// 	close(req.ReadIn)
	// }
	req.Conn.Close()
	delete(RoxyServer.Conns, req.Conn)
	TotalClients--
	// log.Println("CLOSING CONNECTION")
}

func (req *Request) HandleIncoming() {
	// log.Println("GET RIAK CONN")
	rconn := GetRiakConn()
	// log.Println("OBTAINED A RIAK CONNECTION")
	defer rconn.Release()
ReWrite:
	// log.Println("Trying to write")
	rconn.Conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err := rconn.Conn.Write(req.SharedBuffer.Bytes()[:req.msgLen+4])
	// log.Println("Wrote")
	if err != nil {
		log.Println("Errored writing to Riak")
		rconn.Conn.Close()
		time.Sleep(10)
		rconn.ReDialConn()

		// log.Println("Rewrite")
		goto ReWrite
	}
	// log.Println("Wrote: ", req.SharedBuffer.Bytes()[:req.msgLen+4])
Receive:
	var readIn int = 0
	var b int = 0
	retries := 0
ReadLen:
	req.msgLen = 0
	rconn.Conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	b, err = rconn.Conn.Read(req.SharedBuffer.Bytes()[readIn:4])
	if err != nil {
		log.Println("ERROR READING FROM RIAK: ", err)
		nerr, ok := err.(net.Error)
		if retries <= 3 && err != io.EOF && ok && nerr.Temporary() {
			retries++
			goto ReadLen
		}
		req.Write(ErrorResp)
		rconn.Conn.Close()
		time.Sleep(10)
		rconn.ReDialConn()
		req.Close()
		return
	}
	readIn += b
	if readIn < 4 {
		goto ReadLen
	}
	req.msgLen = ParseMessageLength(req.SharedBuffer.Bytes()[:4])
	// log.Println("RiakResp Msg Len: ", req.msgLen)
	req.checkBufferSize(req.msgLen)
	var bytesRead int = 0
	for {
		if bytesRead >= req.msgLen {
			break
		}
		b, err = rconn.Conn.Read(req.SharedBuffer.Bytes()[4+bytesRead:])
		if err != nil {
			nerr, ok := err.(net.Error)
			if err != io.EOF && ok && nerr.Temporary() {
				// log.Println("Temp Error!")
				time.Sleep(300 * time.Millisecond)
				continue
			}
			// log.Println("Returning??????")
			req.Write(ErrorResp)
			rconn.Conn.Close()
			time.Sleep(10)
			rconn.ReDialConn()
			req.Close()
			return
		}
		bytesRead += b
	}
	// log.Println("Recieved: ", numToCommand[int(req.SharedBuffer.Bytes()[4])])
	startTime := time.Now()
	req.Write(req.SharedBuffer.Bytes()[:req.msgLen+4])
	endTime := time.Now()
	// log.Println("Riak Response: ", req.SharedBuffer.Bytes()[:req.msgLen+4])
	// log.Println("Riak RespMsg: ", numToCommand[int(req.SharedBuffer.Bytes()[4])])
	go req.trackLatency(startTime, endTime)
	go req.trackCmdsProcessed()
	cmd := req.SharedBuffer.Bytes()[4]
	if cmd == commandToNum["RpbMapRedResp"] &&
		!bytes.Equal(req.SharedBuffer.Bytes()[:req.msgLen+4], MapRedDone) {
		goto Receive
	}
}

func (req *Request) Reader() {
	err := req.Read()
	if err != nil {
		req.Close()
		return
	}
	req.ReadIn <- true
}

func (req *Request) Sender() {
	for _ = range req.ReadIn {
		code := req.SharedBuffer.Bytes()[4]
		// log.Println("CODE: ", code)
		if code == commandToNum["RpbPingReq"] {
			// log.Println("<<<<<<<QUICK PING RESP>>>>>>>>")
			req.Write(PingResp)
		} else {
			req.HandleIncoming()
		}
		req.Reader()
	}
}
