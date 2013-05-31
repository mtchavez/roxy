package roxy

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
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
	p.m.Lock()
	if len(p.WaitQueue) > 0 {
		ch := p.WaitQueue[0]
		p.WaitQueue = p.WaitQueue[1:]
		p.m.Unlock()
		ch <- rconn
		return true, nil
	}
	p.ConnQueue[p.index] = rconn
	p.index += 1
	p.m.Unlock()
	return true, nil
}

func (p *Pool) Pop() (*RiakConn, error) {
	p.m.Lock()
	if p.index > 0 {
		p.index = p.index - 1
		rconn := p.ConnQueue[p.index]
		rconn.Lock()
		p.m.Unlock()
		return rconn, nil
	}
	ch := make(chan *RiakConn, 1)
	p.WaitQueue = append(p.WaitQueue, ch)
	p.m.Unlock()
	rconn := <-ch
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

	RiakPool.index = 0
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
	// log.Println("[[[[[[[[[[[[[Re-Dialing The Riak]]]]]]]]]]]]]]")
	serverString := riakServerString()
	tries := 0
ReDial:
	if tries >= 5 {
		return
	}
	conn, err := dialServer(serverString)
	if err != nil {
		tries++
		time.Sleep(5 * time.Second)
		goto ReDial
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
