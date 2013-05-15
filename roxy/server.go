package main

import (
	"fmt"
	"net"
	// "time"
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

func (req *Request) Read(buffer []byte) (bytesRead int, read bool) {
	read = true
	var err error
	bytesRead, err = req.Conn.Read(buffer)
	if err != nil {
		req.Write([]byte("FAILED"))
		req.Close()
		fmt.Println("Error reading")
		read = false
	}
	fmt.Printf("Read %v bytes\n", bytesRead)
	return
}

func (req *Request) Write(buffer []byte) {
	fmt.Printf("\n[Writing]\n")
	fmt.Println(string(buffer))
	req.Conn.Write(buffer)
	fmt.Println("[Finished Writing]\n")
}

func RequestHandler(conn net.Conn) {
	quit := make(chan bool)
	in := make(chan []byte)
	out := make(chan []byte)
	req := &Request{Conn: conn, Quit: quit}
	go RequestReader(req, in, out)
	go RequestResponder(req, out)
	go RequestSender(req, in)
}

func RequestSender(req *Request, in chan []byte) {
	fmt.Println("[Begin] RequestSender")
	// <-in
	output := <-in
	fmt.Println(output)
	// req.Write(output)
	fmt.Println("[End] RequestSender")
	// req.Close()
}

func RequestResponder(req *Request, out chan []byte) {
	fmt.Println("[Begin] RequestResponder")
	response := <-out
	req.Write(response)
	fmt.Println("[End] RequestResponder")
	req.Close()
}

func RequestReader(req *Request, in chan []byte, out chan []byte) {
	for {
		buf := make([]byte, 2048)
		n, ok := req.Read(buf)
		if ok {
			// fmt.Printf("Buffer: \n%v\n", string(buf))
			in <- buf[:n]
			req.Resp = []byte("OK")
			out <- req.Resp
		} else {
			req.Resp = []byte("FAILED")
			out <- req.Resp
			req.Close()
			return
		}
		if <-req.Quit {
			fmt.Printf("GOING TO QUIT\n\n")
			// req.Conn.Close()
			// req.Close()
			return
		}
	}
}

func main() {
	listenerConn, netErr := net.Listen("tcp", ":8098")
	if netErr != nil {
		fmt.Println("Error connection to port 8098")
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
