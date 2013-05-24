package roxy

import (
	"log"
	"net"
	"runtime"
	"strconv"
)

func roxyServerString() string {
	roxy_ip := Configuration.Doc.GetString("roxy.ip", "127.0.0.1")
	roxy_port := Configuration.Doc.GetInt("roxy.port", 8088)
	return roxy_ip + ":" + strconv.Itoa(roxy_port)
}

func Setup(configpath string) {
	ParseConfig(configpath)
	runtime.GOMAXPROCS(8)
	poolSize := Configuration.Doc.GetInt("riak.pool_size", 5)
	FillPool(poolSize)
}

func RunProxy() {
	server_string := roxyServerString()
	listenerConn, netErr := net.Listen("tcp", server_string)
	if netErr != nil {
		log.Println("Error connecting to  ", server_string)
		return
	}

	defer listenerConn.Close()
	for {
		conn, err := listenerConn.Accept()
		if err != nil {
			log.Println("Connection error: ", err)
			continue
		}
		go RequestHandler(conn)
	}
}
