package roxy

import (
	"fmt"
	"net"
	"runtime"
	"strconv"
)

func roxyServerString() string {
	roxy_ip := Configuration.Doc.GetString("roxy.development.ip", "127.0.0.1")
	roxy_port := Configuration.Doc.GetInt("roxy.development.port", 8088)
	return roxy_ip + ":" + strconv.Itoa(roxy_port)
}

func setup() {
	ParseConfig()
	runtime.GOMAXPROCS(4)
}

func Run() {
	setup()
	server_string := roxyServerString()
	listenerConn, netErr := net.Listen("tcp", server_string)
	if netErr != nil {
		fmt.Println("Error connecting to  ", server_string)
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
