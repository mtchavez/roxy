package roxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"time"
)

type Request struct {
	handler     *ClientHandler
	background  bool
	mapReducing bool
}

var ReadTimeout float64

func (req *Request) Process() {
	// Determine type of Request
	code := req.handler.Buff.Bytes()[4]
	req.background = false
	req.mapReducing = (code == commandToNum["RpbMapRedReq"])

	if code == commandToNum["RpbPingReq"] {
		req.handler.Write(PingResp)
	} else if code == commandToNum["RpbGetServerInfoReq"] {
		req.handler.Write(ServerInfoResp)
	} else if false && code == commandToNum["RpbGetReq"] {
		startTime := time.Now()
		req.HandleGetReq()
		endTime := time.Now()
		go req.trackTiming(startTime, endTime, "roxy.read.get_req")
	} else if BgHandler.canProcess() &&
		!req.mapReducing &&
		code == commandToNum["RpbPutReq"] {
		req.handler.Write(PutResp)
		newHandler := &ClientHandler{
			Buff:   bytes.NewBuffer(req.handler.Buff.Bytes()),
			msgLen: req.handler.msgLen,
			m:      req.handler.m,
		}
		newRequest := &Request{
			handler:    newHandler,
			background: true,
		}
		go req.trackCountForKey("roxy.bgprocs.throughput")
		go BgHandler.handleInBg(newRequest)
	} else {
		req.HandleIncoming()
	}

	req.handler.done <- true
}

func RunGet(req *Request, rconn *RiakConn, finished chan []byte) {
	start := time.Now()
	var err error

	err = req.ReadRiakLenBuff(rconn)
	if err != nil {
		rconn.Release()
		return
	}
	rconn.msgLen = ParseMessageLength(rconn.Buff.Bytes()[:4])
	rconn.checkBufferSize(rconn.msgLen)

	// Read full message from Riak
	err = req.RiakRecvResp(rconn)
	if err != nil {
		rconn.Release()
		return
	}
	end := time.Now()
	go req.trackTiming(start, end, "roxy.read.riak_time")
	newBuf := rconn.Buff.Bytes()[:rconn.msgLen+4]
	rconn.Release()
	finished <- newBuf
}

func (req *Request) HandleGetReq() {
	var err error
	var rconn *RiakConn
	var recBuf []byte
	var finished chan []byte
	var retry bool
	var startTime, endTime time.Time
	tries := 1
	received := false
	dur, _ := time.ParseDuration(fmt.Sprintf("%fms", ReadTimeout))
	finished = make(chan []byte, 2)

ReProcess:
	retry = false
	// Write client request to Riak
	rconn = GetRiakConn()
	startTime = time.Now()
	err = req.RiakWrite(rconn)
	if err != nil {
		log.Println("[HandleGetReq] Writing Error Resp: ", err)
		req.handler.Write(ErrorResp)
		return
	}
	endTime = time.Now()
	go req.trackTiming(startTime, endTime, "roxy.write.riak_time")

	go RunGet(req, rconn, finished)

RiakRead:
	for {
		select {
		case <-time.After(dur):
			if tries < 2 {
				go req.trackCountForKey("roxy.read_timeouts.total")
				retry = true
				break RiakRead
			}
		case val := <-finished:
			if !received {
				received = true
				recBuf = val
				goto Respond
			} else {
				return
			}
			break RiakRead
		}
	}

	if retry {
		tries++
		goto ReProcess
	}

Respond:
	startTime = time.Now()
	req.handler.Write(recBuf)
	endTime = time.Now()
	go req.trackTiming(startTime, endTime, "roxy.write.time")
	go req.trackCountForKey("roxy.commands.processed")
}

// Takes the read in message from the client and writes it to Riak.
// After a successfull write it reads the response from Riak and
// sends that response back to the client
func (req *Request) HandleIncoming() {
	var err error
	var cmd byte
	var startTime, endTime time.Time

	rconn := GetRiakConn()
	defer rconn.Release()

	// Write client request to Riak
	startTime = time.Now()
	err = req.RiakWrite(rconn)
	if err != nil {
		log.Println("[HandleIncomming] Writing Error Resp: ", err)
		req.handler.Write(ErrorResp)
		if !req.background {
			req.handler.Close()
		}
		return
	}
	endTime = time.Now()
	go req.trackTiming(startTime, endTime, "roxy.write.riak_time")

	// Read/Receive response from Riak
Receive:

	// Read in buffer to determine messag length
	err = req.ReadRiakLenBuff(rconn)

	if err != nil {
		return
	}
	rconn.msgLen = ParseMessageLength(rconn.Buff.Bytes()[:4])
	rconn.checkBufferSize(rconn.msgLen)

	// Read full message from Riak
	err = req.RiakRecvResp(rconn)
	if err != nil {
		return
	}

	// Write Riak response to client
	if !req.background {
		startTime = time.Now()
		req.handler.Write(rconn.Buff.Bytes()[:rconn.msgLen+4])
		endTime = time.Now()
		go req.trackTiming(startTime, endTime, "roxy.write.time")
	}
	go req.trackCountForKey("roxy.commands.processed")

	// Go back to Receive to continually read responses
	// from Riak. Happens when doing a map reduce
	cmd = rconn.Buff.Bytes()[4]
	if cmd == commandToNum["RpbMapRedResp"] &&
		!bytes.Equal(rconn.Buff.Bytes()[:rconn.msgLen+4], MapRedDone) {
		goto Receive
	}
	if cmd == commandToNum["RpbListKeysResp"] &&
		!bytes.Equal(rconn.Buff.Bytes()[:rconn.msgLen+4], ListKeysDone) {
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

// Writes the contents of the client request in SharedBuffer to Riak
func (req *Request) RiakWrite(rconn *RiakConn) (err error) {
	var nerr net.Error
	var ok, tmpOrTimeErr bool
	retries := 0

ReWrite:
	rconn.Conn.SetDeadline(time.Now().Add(3 * time.Second))
	_, err = rconn.Conn.Write(req.handler.Buff.Bytes()[:req.handler.msgLen+4])
	if err != nil {
		nerr, ok = err.(net.Error)
		tmpOrTimeErr = ok && (nerr.Temporary() || nerr.Timeout())
		if retries <= 3 && (err != io.EOF || tmpOrTimeErr) {
			retries++
			time.Sleep(time.Duration(retries*10) * time.Millisecond)
			goto ReWrite
		}
		log.Println("[RiakWrite] Error writing, closing Riak Conn")
		rconn.Conn.Close()
		time.Sleep(time.Duration(retries*10) * time.Millisecond)
		rconn.ReDialConn()
	}
	return
}

// Reads first 4 bytes from a Riak response into the SharedBuffer
// to determine the message length for the Request.
func (req *Request) ReadRiakLenBuff(rconn *RiakConn) (err error) {
	var readIn, b int = 0, 0
	var nerr net.Error
	var ok, tmpOrTimeErr bool
	retries := 0

ReadLen:
	err = nil
	rconn.msgLen = 0
	rconn.Conn.SetDeadline(time.Now().Add(3 * time.Second))
	b, err = rconn.Conn.Read(rconn.Buff.Bytes()[readIn:4])
	if err != nil {
		nerr, ok = err.(net.Error)
		tmpOrTimeErr = ok && (nerr.Temporary() || nerr.Timeout())
		if retries <= 3 && (err != io.EOF || tmpOrTimeErr) {
			retries++
			time.Sleep(time.Duration(retries*10) * time.Millisecond)
			goto ReadLen
		}
		if !req.background {
			log.Println("[ReadRiakLenBuff] Writing Error Resp: ", err)
			req.handler.Write(ErrorResp)
			req.handler.Close()
		}
		if err != io.EOF {
			log.Println("[ReadRiakLenBuff] Error reading, closing Riak Conn ", err)
		}
		rconn.Conn.Close()
		time.Sleep(time.Duration(retries*10) * time.Millisecond)
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
	var nerr net.Error
	var ok, tmpOrTimeErr bool

	for {
		err = nil
		if bytesRead >= rconn.msgLen {
			break
		}
		rconn.Conn.SetDeadline(time.Now().Add(3 * time.Second))
		maxBytes := int(math.Min(float64(rconn.msgLen-bytesRead), float64(8192)))
		b, err = rconn.Conn.Read(rconn.Buff.Bytes()[4+bytesRead : 4+bytesRead+maxBytes])
		if err != nil {
			nerr, ok = err.(net.Error)
			tmpOrTimeErr = ok && (nerr.Temporary() || nerr.Timeout())
			if err != io.EOF || tmpOrTimeErr {
				log.Println("Error is Temporary() or Timeout() ", err, nerr)
				time.Sleep(1 * time.Millisecond)
				continue
			}
			if !req.background {
				log.Println("[RiakRecvResp] Writing Error Resp: ", err)
				req.handler.Write(ErrorResp)
				req.handler.Close()
			}
			if err != io.EOF {
				log.Println("[RiakWrite] Error receiving msg, closing Riak Conn ", err)
			}
			rconn.Conn.Close()
			time.Sleep(10 * time.Millisecond)
			rconn.ReDialConn()
			return
		}
		bytesRead += b
	}
	return
}
