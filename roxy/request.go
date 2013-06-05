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

// Request is a container for a client connection
// that has a buffer to manage client and riak read/write
type Request struct {
	Conn         net.Conn
	ReadIn       chan bool
	SharedBuffer *bytes.Buffer
	msgLen       int
	statsEnabled bool
	StatsClient  *statsite.Client
	mapReducing  bool
	responded    bool
	closeReq     chan bool
}

// RequestHandler makes a new request for an incomming client connection.
// If stats are enabled then a statsite client will be attached.
// The result of calling this method is a Sender() and Reader() will
// be running in separate go routines to handle client reading and writing.
func RequestHandler(conn net.Conn) {
	in := make(chan bool, 8)
	req := &Request{
		Conn:         conn,
		ReadIn:       in,
		SharedBuffer: bytes.NewBuffer(make([]byte, 64000)),
		msgLen:       0,
		responded:    false,
		closeReq:     make(chan bool, 1),
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

// Reader for a Request to read in from the client
func (req *Request) Reader() {
	err := req.Read()
	if err != nil {
		req.Close()
		return
	}
	req.ReadIn <- true
}

// Sender for a Request to listen on ReadIn channel sent from Reader().
// Passes read in message to HandleIncoming()
func (req *Request) Sender() {
	for {
		select {
		case <-req.closeReq:
			return
		case <-req.ReadIn:
			code := req.SharedBuffer.Bytes()[4]
			req.responded = false
			req.mapReducing = (code == commandToNum["RpbMapRedReq"])
			if code == commandToNum["RpbPingReq"] {
				req.Write(PingResp)
			} else if code == commandToNum["RpbGetServerInfoReq"] {
				req.Write(ServerInfoResp)
			} else if BgWrites.canProcess() &&
				!req.mapReducing &&
				code == commandToNum["RpbPutReq"] {
				req.Write(PutResp)
				go BgWrites.sendPut(req.SharedBuffer, req.msgLen)
			} else {
				req.HandleIncoming()
			}
			req.Reader()
		}
	}
}

// Writes buffer to the client for a Request
func (req *Request) Write(buffer []byte) {
	req.Conn.Write(buffer)
}

// Closes a Request. This will close the client connection and
// remove itself from the connections Roxy knows about and
// decrement the total clients by 1
func (req *Request) Close() {
	req.Conn.Close()
	delete(RoxyServer.Conns, req.Conn)
	TotalClients--
	req.closeReq <- true
}

// Read will read from client connection into the SharedBuffer for the Request
func (req *Request) Read() (err error) {
	err = req.ReadInLengthBuffer()
	if err != nil {
		return
	}
	var bytesRead, b int = 0, 0
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
	return
}

// Takes the read in message from the client and writes it to Riak.
// After a successfull write it reads the response from Riak and
// sends that response back to the client
func (req *Request) HandleIncoming() {
	rconn := GetRiakConn()
	defer rconn.Release()
	var err error

	// Write client request to Riak
	err = req.RiakWrite(rconn)
	if err != nil {
		req.Write(ErrorResp)
		req.Close()
		return
	}

	// Read/Receive response from Riak
Receive:

	// Read in buffer to determine messag length
	err = req.ReadRiakLenBuff(rconn)
	if err != nil {
		return
	}
	req.msgLen = ParseMessageLength(req.SharedBuffer.Bytes()[:4])
	req.checkBufferSize(req.msgLen)

	// Read full message from Riak
	err = req.RiakRecvResp(rconn)
	if err != nil {
		return
	}

	// Write Riak response to client
	if !req.responded {
		startTime := time.Now()
		req.Write(req.SharedBuffer.Bytes()[:req.msgLen+4])
		endTime := time.Now()
		go req.trackLatency(startTime, endTime)
	}
	go req.trackCmdsProcessed()

	// Go back to Receive to continually read responses
	// from Riak. Happens when doing a map reduce
	// cmd := req.SharedBuffer.Bytes()[4]
	if req.mapReducing &&
		!bytes.Equal(req.SharedBuffer.Bytes()[:req.msgLen+4], MapRedDone) {
		goto Receive
	}
	req.mapReducing = false
}

// Parses the message length from first 4 bytes of message
func ParseMessageLength(buffer []byte) int {
	var resplength int32
	resplength_buff := bytes.NewBuffer(buffer[0:4])
	err := binary.Read(resplength_buff, binary.BigEndian, &resplength)
	if err != nil {
		log.Println("Error parsing message length: ", err)
	}
	return int(resplength)
}

// Checks the SharedBuffer of the Request against the message length
// of the Riak request. The SharedBuffer is doubled if the message length
// is larger than the length of the buffer.
func (req *Request) checkBufferSize(msglen int) {
	if (msglen + 4) < req.SharedBuffer.Len() {
		return
	}
	req.SharedBuffer.Grow(msglen + 4)
}

// Gets the length buffer from the client request
func (req *Request) ReadInLengthBuffer() (err error) {
	var b int = 0
	readIn := 0
	req.msgLen = 0
ReadLen:
	b, err = req.Conn.Read(req.SharedBuffer.Bytes()[readIn:4])
	if err != nil {
		if err != io.EOF {
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
	req.msgLen = ParseMessageLength(req.SharedBuffer.Bytes()[:4])
	return
}

// Writes the contents of the client request in SharedBuffer to Riak
func (req *Request) RiakWrite(rconn *RiakConn) error {
ReWrite:
	rconn.Conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err := rconn.Conn.Write(req.SharedBuffer.Bytes()[:req.msgLen+4])
	if err != nil {
		log.Println("Errored writing to Riak")
		rconn.Conn.Close()
		time.Sleep(10)
		rconn.ReDialConn()

		goto ReWrite
	}
	return err
}

// Reads first 4 bytes from a Riak response into the SharedBuffer
// to determine the message length for the Request.
func (req *Request) ReadRiakLenBuff(rconn *RiakConn) (err error) {
	var readIn, b int = 0, 0
	retries := 0
ReadLen:
	req.msgLen = 0
	rconn.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	b, err = rconn.Conn.Read(req.SharedBuffer.Bytes()[readIn:4])
	if err != nil {
		nerr, ok := err.(net.Error)
		if retries <= 3 && err != io.EOF && ok && nerr.Temporary() {
			retries++
			goto ReadLen
		}
		log.Println("ERROR READING FROM RIAK: ", err)
		req.Write(ErrorResp)
		rconn.Conn.Close()
		time.Sleep(10)
		rconn.ReDialConn()
		req.Close()
		return err
	}
	readIn += b
	if readIn < 4 {
		goto ReadLen
	}
	return err
}

// Reads in the response from the Riak connection into SharedBuffer.
// Continually reads until full message length is read into buffer.
func (req *Request) RiakRecvResp(rconn *RiakConn) (err error) {
	var bytesRead, b int = 0, 0
	for {
		if bytesRead >= req.msgLen {
			break
		}
		b, err = rconn.Conn.Read(req.SharedBuffer.Bytes()[4+bytesRead:])
		if err != nil {
			nerr, ok := err.(net.Error)
			if err != io.EOF && ok && nerr.Temporary() {
				time.Sleep(300 * time.Millisecond)
				continue
			}
			req.Write(ErrorResp)
			rconn.Conn.Close()
			time.Sleep(10)
			rconn.ReDialConn()
			req.Close()
			return
		}
		bytesRead += b
	}
	return
}
