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
	Conn  net.Conn
	Quit  chan bool
	Resp  []byte
	Error []byte
}

func (req *Request) Close() {
	fmt.Println("Closing connection")
	req.Conn.Close()
	req.Quit <- true
	fmt.Println("Finished closing connection")
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
	buffer = make([]byte, 512)
	_, err = req.Conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading")
		return
	}
	buffer = BufferedResponse(buffer)
	fmt.Println("RequestStruct: ", numToCommand[int(buffer[4])])
	fmt.Println("Read: ", buffer)
	fmt.Println()
	return
}

func (req *Request) Write(buffer []byte) {
	req.Conn.Write(buffer)
	fmt.Println("Wrote: ", buffer)
	fmt.Println()
}

func RequestHandler(conn net.Conn) {
	quit := make(chan bool)
	in := make(chan []byte, 2)
	req := &Request{Conn: conn, Quit: quit}
	go RequestReader(req, in, quit)
	for {
		select {
		case incoming := <-in:
			HandleRequest(req, incoming)
		case <-quit:
			break
		}
	}
}

func HandleRequest(req *Request, incomming []byte) {
	conn, err := net.Dial("tcp", ":8087")
	if err != nil {
		fmt.Println("Error connection to riak")
	}
	_, err = conn.Write(incomming)
	if err != nil {
		fmt.Println("Error writing to riak")
		return
	}
	respraw := make([]byte, 512)
	_, err = conn.Read(respraw)
	fmt.Println("ResponseStruct: ", numToCommand[int(respraw[4])])
	respraw = BufferedResponse(respraw)
	req.Write(respraw)
	conn.Close()
	fmt.Println()
}

func RequestReader(req *Request, in chan []byte, quit chan bool) {
	for {
		select {
		case <-quit:
			break
		default:
			buf, err := req.Read()
			if err != nil {
				req.Close()
				return
			}
			in <- buf
		}
	}
}

func Run() {
	listenerConn, netErr := net.Listen("tcp", ":8088")
	if netErr != nil {
		fmt.Println("Error connection to port 8088")
		return
	}

	defer listenerConn.Close()
	for {
		conn, err := listenerConn.Accept()
		if err != nil {
			fmt.Println("Connection error: ", err)
		}
		go RequestHandler(conn)
	}
}
