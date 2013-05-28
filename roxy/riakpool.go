package roxy

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
)

const (
	SLEEPING = iota
	BUSY
)

var poolcc = make([]*RiakConn, 5)
var poolwq = make([]chan *RiakConn, 0)
var poolm = &sync.Mutex{}

type Pool struct {
	index     int
	ConnQueue []*RiakConn
	WaitQueue []chan *RiakConn
	m         *sync.Mutex
}

type RiakConn struct {
	Conn   *net.TCPConn
	Status int
}

var RiakPool = Pool{
	index:     0,
	ConnQueue: poolcc,
	WaitQueue: poolwq,
	m:         poolm,
}

func (p *Pool) Push(rconn *RiakConn) (bool, error) {
	// mutex := &sync.Mutex{}
	RiakPool.m.Lock()
	fmt.Printf("[PUSH] POOL: &p=%p p.index=%v\n", &RiakPool, RiakPool.index)
	if len(RiakPool.WaitQueue) > 0 {
		ch := RiakPool.WaitQueue[0]
		RiakPool.WaitQueue = RiakPool.WaitQueue[1:]
		RiakPool.m.Unlock()
		ch <- rconn
		return true, nil
	}
	RiakPool.ConnQueue[RiakPool.index] = rconn
	RiakPool.index += 1
	RiakPool.m.Unlock()
	return true, nil
}

func (p *Pool) Pop() (*RiakConn, error) {
	// mutex := &sync.Mutex{}
	RiakPool.m.Lock()
	fmt.Printf("[POP] POOL: &p=%p p.index=%v\n", &RiakPool, RiakPool.index)
	if RiakPool.index > 0 {
		RiakPool.index = RiakPool.index - 1
		rconn := RiakPool.ConnQueue[RiakPool.index]
		rconn.Lock()
		RiakPool.m.Unlock()
		return rconn, nil
	}
	log.Println("\t\tNeed to wait for a connection\n")
	ch := make(chan *RiakConn)
	// TODO: Handle case of at wait queue limit
	RiakPool.WaitQueue = append(RiakPool.WaitQueue, ch)
	log.Println("Waiting for connection")
	rconn := <-ch
	RiakPool.m.Unlock()
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
	log.Printf("\t\t[FillPool] POOL: &p=%p p.index=%v\n", &RiakPool, RiakPool.index)
	// RiakPool = &Pool{0, make([]*RiakConn, num), make([]chan *RiakConn, 0), &sync.Mutex{}}
	RiakPool.ConnQueue = make([]*RiakConn, num)
	serverString := riakServerString()
	for i := 0; i < num; i++ {
		conn, err := dialServer(serverString)
		if err != nil {
			continue
		}
		RiakPool.Push(&RiakConn{conn, SLEEPING})
	}
}

func (rconn *RiakConn) ReDialConn() {
	serverString := riakServerString()
	conn, err := dialServer(serverString)
	if err != nil {
		return
	}
	rconn.Conn = conn
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
