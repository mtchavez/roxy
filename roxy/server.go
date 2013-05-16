package roxy

import (
	"bytes"
	// "code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	// "errors"
	"fmt"
	"net"
)

var commandToNum = map[string]byte{
	"RpbErrorResp":         0,
	"RpbPingReq":           1,
	"RpbPingResp":          2,
	"RpbGetClientIdReq":    3,
	"RpbGetClientIdResp":   4,
	"RpbSetClientIdReq":    5,
	"RpbSetClientIdResp":   6,
	"RpbGetServerInfoReq":  7,
	"RpbGetServerInfoResp": 8,
	"RpbGetReq":            9,
	"RpbGetResp":           10,
	"RpbPutReq":            11,
	"RpbPutResp":           12,
	"RpbDelReq":            13,
	"RpbDelResp":           14,
	"RpbListBucketsReq":    15,
	"RpbListBucketsResp":   16,
	"RpbListKeysReq":       17,
	"RpbListKeysResp":      18,
	"RpbGetBucketReq":      19,
	"RpbGetBucketResp":     20,
	"RpbSetBucketReq":      21,
	"RpbSetBucketResp":     22,
	"RpbMapRedReq":         23,
	"RpbMapRedResp":        24,
}

var numToCommand = map[int]string{
	0:  "RpbErrorResp",
	1:  "RpbPingReq",
	2:  "RpbPingResp",
	3:  "RpbGetClientIdReq",
	4:  "RpbGetClientIdResp",
	5:  "RpbSetClientIdReq",
	6:  "RpbSetClientIdResp",
	7:  "RpbGetServerInfoReq",
	8:  "RpbGetServerInfoResp",
	9:  "RpbGetReq",
	10: "RpbGetResp",
	11: "RpbPutReq",
	12: "RpbPutResp",
	13: "RpbDelReq",
	14: "RpbDelResp",
	15: "RpbListBucketsReq",
	16: "RpbListBucketsResp",
	17: "RpbListKeysReq",
	18: "RpbListKeysResp",
	19: "RpbGetBucketReq",
	20: "RpbGetBucketResp",
	21: "RpbSetBucketReq",
	22: "RpbSetBucketResp",
	23: "RpbMapRedReq",
	24: "RpbMapRedResp",
}

type Request struct {
	Conn  net.Conn
	Quit  chan bool
	Resp  []byte
	Error []byte
}

func (req *Request) Close() {
	fmt.Println("Closing connection")
	req.Conn.Close()
	// req.Quit <- true
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
		req.Close()
		fmt.Println("Error reading")
	}
	fmt.Println("RequestStruct: ", numToCommand[int(buffer[4])])
	buffer = BufferedResponse(buffer)
	return
}

func (req *Request) Write(buffer []byte) {
	fmt.Printf("\n[Writing]\n")
	fmt.Println(buffer)
	req.Conn.Write(buffer)
	fmt.Println("[Finished Writing]\n")
}

func RequestHandler(conn net.Conn) {
	quit := make(chan bool)
	in := make(chan []byte, 2)
	req := &Request{Conn: conn, Quit: quit}
	go RequestReader(req, in)
	go RequestSender(req, in)
}

func RequestSender(req *Request, in chan []byte) {
	for {
		fmt.Println("[Begin] RequestSender")
		output := <-in
		conn, err := net.Dial("tcp", ":8087")
		if err != nil {
			fmt.Println("Error connection to riak")
		}
		_, err = conn.Write(output)
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
		fmt.Println("[End] RequestSender")
	}
}

func RequestReader(req *Request, in chan []byte) {
	for {
		buf, err := req.Read()
		if err == nil {
			fmt.Printf("Buffer: \n%v\n", buf)
			in <- buf
		} else {
			req.Close()
			return
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
