package roxy

import (
	"bytes"
	// "code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	// "errors"
	"fmt"
	"net"
	"strconv"
)

type Request struct {
	Conn     net.Conn
	Quit     chan bool
	Incoming chan []byte
	Resp     []byte
	Error    []byte
}

func RequestHandler(conn net.Conn) {
	quit := make(chan bool)
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

func (req *Request) Close() {
	fmt.Println("Closing connection")
	req.Conn.Close()
	req.Quit <- true
	fmt.Println("Finished closing connection")
}

func riakServerString() string {
	riak_ip := Configuration.Doc.GetString("riak.development.ip", "127.0.0.1")
	riak_port := Configuration.Doc.GetInt("riak.development.port", 8087)
	return riak_ip + ":" + strconv.Itoa(riak_port)
}

func (req *Request) HandleIncoming(incomming []byte) {
	server_string := riakServerString()
	conn, err := net.Dial("tcp", server_string)
	if err != nil {
		fmt.Println("Error connection to riak")
	}
	_, err = conn.Write(incomming)
	if err != nil {
		fmt.Println("Error writing to riak")
		return
	}
	rawresp := make([]byte, 512)
	_, err = conn.Read(rawresp)
	fmt.Println("ResponseStruct: ", numToCommand[int(rawresp[4])])
	rawresp = BufferedResponse(rawresp)
	req.Write(rawresp)
	conn.Close()
	fmt.Println()
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
