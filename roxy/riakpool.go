package roxy

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
)

var RiakPool *Pool

const (
	SLEEPING = iota
	BUSY
)

type Pool struct {
	index       int
	Connections []*RiakConn
}

type RiakConn struct {
	Conn   *net.TCPConn
	Status int
}

func (p *Pool) Push(rconn *RiakConn) (bool, error) {
	mutex := &sync.Mutex{}
	mutex.Lock()
	defer mutex.Unlock()
	if p.index+1 > len(p.Connections) {
		return false, errors.New("Pool is full")
	}
	p.Connections[p.index] = rconn
	p.index += 1
	return true, nil
}

func (p *Pool) Pop() (*RiakConn, error) {
	mutex := &sync.Mutex{}
	mutex.Lock()
	defer mutex.Unlock()
	if p.index == 0 {
		return nil, errors.New("No more connections in pool")
	}
	p.index -= 1
	rconn := p.Connections[p.index]
	rconn.Lock()
	return rconn, nil
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
	rconn.Status = SLEEPING
	RiakPool.Push(rconn)
}

func FillPool(num int) {
	if num <= 0 {
		num = 5
	}
	RiakPool = &Pool{0, make([]*RiakConn, num)}
	serverString := riakServerString()
	for i := 0; i < num; i++ {
		conn, err := dialServer(serverString)
		if err != nil {
			continue
		}
		RiakPool.Push(&RiakConn{conn, SLEEPING})
	}
}

func GetRiakConn() (rconn *RiakConn) {
	var err error
	rconn, err = RiakPool.Pop()
	if err != nil {
		log.Println("Error retrieving riak connection")
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
