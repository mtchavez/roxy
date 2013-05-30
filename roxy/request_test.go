package roxy

import (
	. "launchpad.net/gocheck"
	"net"
	"time"
)

func (s *MySuite) TestValidRequest(c *C) {
	req := &Request{}

	var in chan []byte
	c.Assert(req.ReadIn, FitsTypeOf, in)

	var shared []byte
	c.Assert(req.SharedBuffer, FitsTypeOf, shared)

	var length []byte
	c.Assert(req.LengthBuffer, FitsTypeOf, length)

	c.Assert(req.bytesRead, FitsTypeOf, int(0))
}

func (s *MySuite) TestParseMessageLength(c *C) {
	message := []byte{0, 0, 0, 3, 24, 24, 1}
	length := ParseMessageLength(message)
	c.Assert(length, Equals, 3)

	message = []byte{0, 0, 0, 0, 7}
	length = ParseMessageLength(message)
	c.Assert(length, Equals, 0)
}

func (s *MySuite) TestSmallerMsgCheckBufferSize(c *C) {
	buffer := make([]byte, 16)
	req := &Request{SharedBuffer: buffer}
	c.Assert(req.SharedBuffer, HasLen, 16)
	req.checkBufferSize(8)
	c.Assert(req.SharedBuffer, HasLen, 16)
}

func (s *MySuite) TestLargerMsgCheckBufferSize(c *C) {
	buffer := make([]byte, 4)
	req := &Request{SharedBuffer: buffer}
	c.Assert(req.SharedBuffer, HasLen, 4)
	req.checkBufferSize(8)
	c.Assert(req.SharedBuffer, HasLen, 24)
}

func (s *MySuite) TestMakingRequest(c *C) {
	c.SucceedNow()
	Setup("./config.toml")
	go RunProxy()
	time.Sleep(500 * time.Millisecond)
	conn, _ := net.Dial("tcp", "127.0.0.1:8088")
	ping := []byte{0, 0, 0, 1, 1}
	conn.Write(ping)
}
