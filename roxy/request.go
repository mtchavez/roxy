package roxy

import (
	"bytes"
	// "code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	// "errors"
	"fmt"
	"net"
)

var SharedBuffer []byte

type Request struct {
	Conn     net.Conn
	Quit     chan bool
	Incoming chan []byte
	Resp     []byte
	Error    []byte
}

func RequestHandler(conn net.Conn) {
	quit := make(chan bool, 2)
	in := make(chan []byte, 2)
	req := &Request{Conn: conn, Quit: quit, Incoming: in}
	go req.Reader()
	for {
		select {
		case incoming := <-req.Incoming:
			req.HandleIncoming(incoming)
		case <-req.Quit:
			break
		}
	}
}

func BufferedResponse(buffer []byte) []byte {
	var resplength int32
	resplength_buff := bytes.NewBuffer(buffer[0:4])

	if err := binary.Read(resplength_buff, binary.BigEndian, &resplength); err != nil {
		return buffer
	}
	return buffer[:resplength+4]
}

func (req *Request) Read() (buffer []byte, err error) {
	_, err = req.Conn.Read(SharedBuffer)
	if err != nil {
		// fmt.Println("Error reading")
		return
	}
	buffer = BufferedResponse(SharedBuffer)
	// fmt.Println("RequestStruct: ", numToCommand[int(SharedBuffer[4])])
	// fmt.Println("Read: ", SharedBuffer)
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
		return
	}
	_, err = rconn.Conn.Read(SharedBuffer)
	rconn.Release()
	// fmt.Println("ResponseStruct: ", numToCommand[int(SharedBuffer[4])])
	req.Write(BufferedResponse(SharedBuffer))
	// fmt.Println()
}

func (req *Request) Reader() {
	for {
		select {
		case <-req.Quit:
			break
		default:
			buf, err := req.Read()
			if err != nil {
				req.Close()
				return
			}
			req.Incoming <- buf
		}
	}
}
