package roxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/mtchavez/go-statsite/statsite"
	"io"
	"log"
	"math"
	"net"
	"runtime"
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
	background   bool
	closeReq     chan bool
}

var ReadTimeout float64

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
		background:   false,
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
			req.background = false
			req.mapReducing = (code == commandToNum["RpbMapRedReq"])
			if code == commandToNum["RpbPingReq"] {
				req.Write(PingResp)
			} else if code == commandToNum["RpbGetServerInfoReq"] {
				req.Write(ServerInfoResp)
			} else if code == commandToNum["RpbGetReq"] {
				req.HandleGetReq()
			} else if BgHandler.canProcess() &&
				!req.mapReducing &&
				code == commandToNum["RpbPutReq"] {
				req.Write(PutResp)
				BgHandler.queueToBg(req.SharedBuffer, req.msgLen)
			} else {
				req.HandleIncoming()
			}
			req.Reader()
			runtime.GC()
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
	if !req.background {
		req.closeReq <- true
	}
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
			if !req.background {
				req.Close()
			}
			return
		}
		bytesRead += b
	}
	return
}

func RunGet(req *Request, rconn *RiakConn, finished chan *Request) {
	var err error
	newBytes := make([]byte, 0)
	newBytes = append(newBytes, req.SharedBuffer.Bytes()[:req.msgLen+4]...)
	origBuf := bytes.NewBuffer(newBytes)
	origMsgLen := req.msgLen

	newReq := &Request{SharedBuffer: origBuf, msgLen: origMsgLen}
	err = newReq.ReadRiakLenBuff(rconn)

	if err != nil {
		rconn.Release()
		return
	}
	newReq.msgLen = ParseMessageLength(newReq.SharedBuffer.Bytes()[:4])
	newReq.checkBufferSize(newReq.msgLen)

	// Read full message from Riak
	err = newReq.RiakRecvResp(rconn)
	if err != nil {
		rconn.Release()
		return
	}
	rconn.Release()
	finished <- newReq
}

func (req *Request) HandleGetReq() {
	var err error
	var rconn *RiakConn
	var readReq *Request
	var finished chan *Request
	var retry bool
	dur, _ := time.ParseDuration(fmt.Sprintf("%fms", ReadTimeout))
ReProcess:
	retry = false
	finished = make(chan *Request, 1)
	// Write client request to Riak
	rconn = GetRiakConn()
	startTime := time.Now()
	err = req.RiakWrite(rconn)
	if err != nil {
		log.Println("[HandleGetReq] Writing Error Resp: ", err)
		req.Write(ErrorResp)
		return
	}
	endTime := time.Now()
	go req.trackWriteTime(startTime, endTime, "roxy.write.riak_time")

	go RunGet(req, rconn, finished)
RiakRead:
	select {
	case <-time.After(dur):
		retry = true
		finished = nil
		runtime.GC()
		break RiakRead
	case val, ok := <-finished:
		if ok {
			readReq = val
		}
		break RiakRead
	}

	if retry {
		goto ReProcess
	}

	startTime = time.Now()
	readReq.Conn = req.Conn
	readReq.Write(readReq.SharedBuffer.Bytes()[:readReq.msgLen+4])
	endTime = time.Now()
	go readReq.trackWriteTime(startTime, endTime, "roxy.write.time")
	go readReq.trackCmdsProcessed()
}

// Takes the read in message from the client and writes it to Riak.
// After a successfull write it reads the response from Riak and
// sends that response back to the client
func (req *Request) HandleIncoming() {
	rconn := GetRiakConn()
	defer func() {
		rconn.Release()
	}()
	var err error

	// Write client request to Riak
	startTime := time.Now()
	err = req.RiakWrite(rconn)
	if err != nil {
		log.Println("[HandleIncomming] Writing Error Resp: ", err)
		req.Write(ErrorResp)
		if !req.background {
			req.Close()
		}
		return
	}
	endTime := time.Now()
	go req.trackWriteTime(startTime, endTime, "roxy.write.riak_time")

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
	if !req.background {
		startTime = time.Now()
		req.Write(req.SharedBuffer.Bytes()[:req.msgLen+4])
		endTime = time.Now()
		go req.trackWriteTime(startTime, endTime, "roxy.write.time")
	}
	go req.trackCmdsProcessed()

	// Go back to Receive to continually read responses
	// from Riak. Happens when doing a map reduce
	cmd := req.SharedBuffer.Bytes()[4]
	if cmd == commandToNum["RpbMapRedResp"] &&
		!bytes.Equal(req.SharedBuffer.Bytes()[:req.msgLen+4], MapRedDone) {
		goto Receive
	}
	req.mapReducing = false
}

// Parses the message length from first 4 bytes of message
func ParseMessageLength(buffer []byte) int {
	var resplength int32
	resplength_buff := bytes.NewBuffer(buffer[:4])
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
	if (msglen + 4) < cap(req.SharedBuffer.Bytes()) {
		return
	}
	req.SharedBuffer.Grow(msglen + 10)
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
			if !req.background {
				req.Close()
			}
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
func (req *Request) RiakWrite(rconn *RiakConn) (err error) {
ReWrite:
	rconn.Conn.SetDeadline(time.Now().Add(60 * time.Second))
	_, err = rconn.Conn.Write(req.SharedBuffer.Bytes()[:req.msgLen+4])
	if err != nil {
		log.Println("[RiakWrite] Error writing, closing Riak Conn")
		rconn.Conn.Close()
		time.Sleep(10)
		rconn.ReDialConn()

		goto ReWrite
	}
	return
}

// Reads first 4 bytes from a Riak response into the SharedBuffer
// to determine the message length for the Request.
func (req *Request) ReadRiakLenBuff(rconn *RiakConn) (err error) {
	var readIn, b int = 0, 0
	retries := 0
ReadLen:
	err = nil
	req.msgLen = 0
	rconn.Conn.SetDeadline(time.Now().Add(60 * time.Second))
	b, err = rconn.Conn.Read(req.SharedBuffer.Bytes()[readIn:4])
	if err != nil {
		nerr, ok := err.(net.Error)
		tmpOrTimeErr := ok && (nerr.Temporary() || nerr.Timeout())
		if retries <= 3 && (err != io.EOF || tmpOrTimeErr) {
			log.Println("RETRY RIAK READ: ", err)
			rconn.ReDialConn()
			retries++
			goto ReadLen
		}
		if !req.background {
			log.Println("[ReadRiakLenBuff] Writing Error Resp: ", err)
			req.Write(ErrorResp)
			req.Close()
		}
		if err != io.EOF {
			log.Println("[ReadRiakLenBuff] Error reading, closing Riak Conn ", err)
		}
		rconn.Conn.Close()
		time.Sleep(10)
		rconn.ReDialConn()
		return
	}
	readIn += b
	if readIn < 4 {
		goto ReadLen
	}
	return
}

// Reads in the response from the Riak connection into SharedBuffer.
// Continually reads until full message length is read into buffer.
func (req *Request) RiakRecvResp(rconn *RiakConn) (err error) {
	var bytesRead, b int = 0, 0
	for {
		err = nil
		if bytesRead >= req.msgLen {
			break
		}
		rconn.Conn.SetDeadline(time.Now().Add(60 * time.Second))
		maxBytes := int(math.Min(float64(req.msgLen-bytesRead), float64(8192)))
		b, err = rconn.Conn.Read(req.SharedBuffer.Bytes()[4+bytesRead : 4+bytesRead+maxBytes])
		if err != nil {
			nerr, ok := err.(net.Error)
			tmpOrTimeErr := ok && (nerr.Temporary() || nerr.Timeout())
			if err != io.EOF || tmpOrTimeErr {
				log.Println("Error is Temporary() or Timeout() ", err, nerr)
				time.Sleep(300 * time.Millisecond)
				continue
			}
			if !req.background {
				log.Println("[RiakRecvResp] Writing Error Resp: ", err)
				req.Write(ErrorResp)
				req.Close()
			}
			if err != io.EOF {
				log.Println("[RiakWrite] Error receiving msg, closing Riak Conn ", err)
			}
			rconn.Conn.Close()
			time.Sleep(10)
			rconn.ReDialConn()
			return
		}
		bytesRead += b
	}
	return
}
