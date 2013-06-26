package roxy

import (
	. "launchpad.net/gocheck"
	"net"
	"time"
)

func (s *MySuite) TestValidRequest(c *C) {
	req := &Request{}

	var boolean bool
	c.Assert(req.background, FitsTypeOf, boolean)
	c.Assert(req.mapReducing, FitsTypeOf, boolean)

	var handler *ClientHandler
	c.Assert(req.handler, FitsTypeOf, handler)
}

func (s *MySuite) TestParseMessageLength(c *C) {
	message := []byte{0, 0, 0, 3, 24, 24, 1}
	length := ParseMessageLength(message)
	c.Assert(length, Equals, 3)

	message = []byte{0, 0, 0, 0, 7}
	length = ParseMessageLength(message)
	c.Assert(length, Equals, 0)

	message = []byte{0, 0, 32, 152, 24}
	println(message)
	length = ParseMessageLength(message)
	c.Assert(length, Equals, 8344)
}

func (s *MySuite) TestMakingRequest(c *C) {
	RoxyServer = Server{}
	Setup("./config.toml")
	go RunProxy()
	time.Sleep(300 * time.Millisecond)
	conn, _ := net.Dial("tcp", "127.0.0.1:8088")
	ping := []byte{0, 0, 0, 1, 1}
	conn.Write(ping)
	buff := make([]byte, 5)
	conn.Read(buff)
	c.Assert(buff, DeepEquals, []byte{0, 0, 0, 1, 2})
	RoxyServer.Shutdown()
}
