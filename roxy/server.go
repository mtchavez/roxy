package roxy

import (
	"fmt"
	"net"
)

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
