package roxy

import (
	"bytes"
	// "code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	// "errors"
	"fmt"
	"net"
)

type Request struct {
	Conn         net.Conn
	ReadIn       chan []byte
	Quit         chan bool
	SharedBuffer []byte
	LengthBuffer []byte
}

func RequestHandler(conn net.Conn) {
	quit := make(chan bool)
	in := make(chan []byte, 8)
	sharedbuffer := make([]byte, 64000)
	lenbuffer := make([]byte, 4)
	req := &Request{
		Conn:         conn,
		ReadIn:       in,
		Quit:         quit,
		SharedBuffer: sharedbuffer,
		LengthBuffer: lenbuffer}
	go req.Sender()
	go req.Reader()
}

func ParseMessageLength(buffer []byte) int {
	var resplength int32
	resplength_buff := bytes.NewBuffer(buffer[0:4])

	if err := binary.Read(resplength_buff, binary.BigEndian, &resplength); err != nil {
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
		// fmt.Println("Error reading")
		return
	}
	// fmt.Println(LengthBuffer)
	msglen := ParseMessageLength(req.LengthBuffer)
	// fmt.Println(msglen)
	req.checkBufferSize(msglen)
	// buffer = SharedBuffer[:msglen]
	_, err = req.Conn.Read(req.SharedBuffer)
	if err != nil {
		// fmt.Println("Error reading")
		return
	}
	buffer = append(req.LengthBuffer, req.SharedBuffer[:msglen]...)
	// buffer = BufferedResponse(SharedBuffer)
	// fmt.Println("RequestStruct: ", numToCommand[int(SharedBuffer[4])])
	// fmt.Println("Read: ", buffer)
	// fmt.Println()
	return
}

func (req *Request) Write(buffer []byte) {
	req.Conn.Write(buffer)
	// fmt.Println("Wrote: ", buffer)
	// fmt.Println()
}

func (req *Request) Close() {
	// fmt.Println("Closing connection")
	req.Conn.Close()
	req.Quit <- true
	// fmt.Println("Finished closing connection")
}

func (req *Request) HandleIncoming(incomming []byte) {
	rconn := GetRiakConn()
	// fmt.Printf("Incomming::: %v\n", rconn)
	_, err := rconn.Conn.Write(incomming)
	if err != nil {
		fmt.Println("Error writing to riak")
		fmt.Println(err)
		return
	}
	_, err = rconn.Conn.Read(req.LengthBuffer)
	// fmt.Println("ResponseStruct: ", numToCommand[int(SharedBuffer[4])])
	msglen := ParseMessageLength(req.LengthBuffer)
	req.checkBufferSize(msglen)
	_, err = rconn.Conn.Read(req.SharedBuffer)
	rconn.Release()
	// fmt.Println("Writing: ", SharedBuffer[:msglen])
	req.Write(append(req.LengthBuffer, req.SharedBuffer[:msglen]...))
	// fmt.Println()
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
