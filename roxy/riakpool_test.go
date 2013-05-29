package roxy

import (
	. "launchpad.net/gocheck"
	"net"
)

func (s *MySuite) TestValidRiakConn(c *C) {
	rconn := RiakConn{}
	var tcp *net.TCPConn
	c.Assert(rconn.Conn, FitsTypeOf, tcp)
	c.Assert(rconn.Status, FitsTypeOf, int(0))
}

func (s *MySuite) TestValidRiakPool(c *C) {
	c.Assert(RiakPool.ConnQueue, FitsTypeOf, []*RiakConn{})
}

func (s *MySuite) TestRiakServerString(c *C) {
	ParseConfig("./config.toml")
	c.Assert(riakServerString(), Equals, "127.0.0.1:8087")
}

func (s *MySuite) TestFillPool(c *C) {
	ParseConfig("./config.toml")

	c.Assert(RiakPool.ConnQueue, HasLen, 5)
	FillPool(-1)
	c.Assert(RiakPool.ConnQueue, HasLen, 5)

	RiakPool.ConnQueue = []*RiakConn{}

	c.Assert(RiakPool.ConnQueue, HasLen, 0)
	FillPool(0)
	c.Assert(RiakPool.ConnQueue, HasLen, 5)

	FillPool(15)
	c.Assert(RiakPool.ConnQueue, HasLen, 15)

	c.Assert(RiakPool.ConnQueue[0], FitsTypeOf, &RiakConn{})
}

func (s *MySuite) TestGetRiakConn(c *C) {
	ParseConfig("./config.toml")
	FillPool(0)
	rconn := GetRiakConn()
	c.Assert(rconn, FitsTypeOf, &RiakConn{})
	c.Assert(rconn.Status, Equals, BUSY)
	var tcp *net.TCPConn
	c.Assert(rconn.Conn, FitsTypeOf, tcp)
}

func (s *MySuite) TestRiakConnLock(c *C) {
	rconn := &RiakConn{}
	c.Assert(rconn.Status, Equals, SLEEPING)
	rconn.Lock()
	c.Assert(rconn.Status, Equals, BUSY)
}

func (s *MySuite) TestRiakConnRelease(c *C) {
	Setup("./config.toml")
	rconn, _ := RiakPool.Pop()
	rconn.Status = BUSY
	rconn.Release()
	c.Assert(rconn.Status, Equals, SLEEPING)
}
