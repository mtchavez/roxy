package roxy

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
)

var RiakPool []*RiakConn

const (
	SLEEPING = iota
	BUSY
)

type RiakConn struct {
	Conn   *net.TCPConn
	Status int
}

func (rconn *RiakConn) String() string {
	fmt.Println("[RiakConn] ", &rconn)
	fmt.Println("conn=", rconn.Conn)
	fmt.Println("status=" + strconv.Itoa(rconn.Status))
	return ""
}

func (rconn *RiakConn) Lock() {
	rconn.Status = BUSY
}

func (rconn *RiakConn) Release() {
	mutex := &sync.Mutex{}
	mutex.Lock()
	defer mutex.Unlock()
	rconn.Status = SLEEPING
	RiakPool = append(RiakPool, rconn)
}

func FillPool(num int) {
	if num <= 0 {
		num = 5
	}
	serverString := riakServerString()
	RiakPool = make([]*RiakConn, num)
	for i := 0; i < num; i++ {
		conn, err := dialServer(serverString)
		if err != nil {
			continue
		}
		RiakPool[i] = &RiakConn{conn, SLEEPING}
	}
}

func GetRiakConn() (rconn *RiakConn) {
	mutex := &sync.Mutex{}
	mutex.Lock()
	defer mutex.Unlock()
	// TODO: Handle case of all connections
	// in BUSY state
	for i := 0; i < len(RiakPool); i++ {
		rconn = RiakPool[i]
		if rconn.Status == SLEEPING {
			rconn.Lock()
			RiakPool = RiakPool[1:]
			return
		}
	}
	return
}

func dialServer(server string) (conn *net.TCPConn, err error) {
	var tcpaddr *net.TCPAddr
	tcpaddr, err = net.ResolveTCPAddr("tcp", server)
	if err != nil {
		log.Println("[RiakPool.dialServer] Error resolving Riak addr: ", err)
		return
	}

	conn, err = net.DialTCP("tcp", nil, tcpaddr)

	if err != nil {
		log.Println("[RiakPool.dialServer] Error connecting to Riak: ", err)
		return
	}
	conn.SetKeepAlive(true)
	return
}

func riakServerString() string {
	riak_ip := Configuration.Doc.GetString("riak.ip", "127.0.0.1")
	riak_port := Configuration.Doc.GetInt("riak.port", 8087)
	return riak_ip + ":" + strconv.Itoa(riak_port)
}
