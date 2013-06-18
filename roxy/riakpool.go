package roxy

import (
	"bytes"
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

// Struct for a Pool that contains a stack of Riak connections
// and a WaitQueue that is used when no Riak connections are
// available.
type Pool struct {
	index     int
	ConnQueue []*RiakConn
	WaitQueue []chan *RiakConn
	m         *sync.Mutex
}

// Struct to hold a connection to Riak and an internal BUSY or SLEEPING status
type RiakConn struct {
	Conn   *net.TCPConn
	Status int
	Buff   *bytes.Buffer
	msgLen int
}

var RiakPool = Pool{
	index:     0,
	ConnQueue: poolcc,
	WaitQueue: poolwq,
	m:         poolm,
}

// Pushes a RiakConn back onto the stack.
// If there is anything in the WaitQueue the RiakConn is sent over a
// channel to the next waiting request instead of being pushed onto the stack.
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

// Pops a RiakConn off the stack and returns it.
// If the stack is empty a channel is made and listens for a RiakConn
// to be sent when one becomes available.
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

// Sets RiakConn status to BUSY
func (rconn *RiakConn) Lock() {
	rconn.Status = BUSY
}

// Sets RiakConn status to SLEEPING and pushes it back onto the stack
func (rconn *RiakConn) Release() {
	rconn.Status = SLEEPING
	RiakPool.Push(rconn)
}

// Checks the Buff of the RiakConn against the message length
// of the Riak request. The Buff is doubled if the message length
// is larger than the length of the buffer.
func (rconn *RiakConn) checkBufferSize(msglen int) {
	if (msglen + 4) < cap(rconn.Buff.Bytes()) {
		return
	}
	rconn.Buff.Grow(msglen + 10)
}

// FillPool takes in a number of connections to make to Riak and
// make available on the stack of Riak connections
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
		RiakPool.Push(&RiakConn{conn, SLEEPING, bytes.NewBuffer(make([]byte, 64000)), 0})
	}
}

// Dials Riak and saves returned connection onto a RiakConn.
// This will try to connect to Riak up to 5 times
func (rconn *RiakConn) ReDialConn() {
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

// Convenience method to retun next RiakConn from stack
func GetRiakConn() (rconn *RiakConn) {
	var err error
	rconn, err = RiakPool.Pop()
	if err != nil {
		log.Println("Error retrieving riak connection")
	}
	return
}

// Given an address to Riak this will resolve that address
// and attempt to get connection to Riak. If successful
// a net.TCPConn is returned with no error otherwise an error will be returned
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
