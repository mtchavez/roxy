package roxy

import (
	"bytes"
	. "launchpad.net/gocheck"
	"net"
	"time"
)

func (s *MySuite) TestValidRequest(c *C) {
	req := &Request{}

	var in chan bool
	c.Assert(req.ReadIn, FitsTypeOf, in)

	var shared *bytes.Buffer
	c.Assert(req.SharedBuffer, FitsTypeOf, shared)
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
	buffer := bytes.NewBuffer(make([]byte, 16))
	req := &Request{SharedBuffer: buffer}
	c.Assert(req.SharedBuffer.Bytes(), HasLen, 16)
	req.checkBufferSize(8)
	c.Assert(req.SharedBuffer.Bytes(), HasLen, 16)
}

func (s *MySuite) TestLargerMsgCheckBufferSize(c *C) {
	buffer := bytes.NewBuffer(make([]byte, 4))
	req := &Request{SharedBuffer: buffer}
	c.Assert(cap(req.SharedBuffer.Bytes()), Equals, 4)
	req.checkBufferSize(8)
	c.Assert(cap(req.SharedBuffer.Bytes())+req.SharedBuffer.Len(), Equals, 24)
}

func (s *MySuite) TestReadInLengthBuffer(c *C) {
	fn := func(listener net.Listener, ch chan net.Conn, close chan bool) {
		conn, _ := net.Dial("tcp", "127.0.0.1:8089")
		cn := <-ch
		req := &Request{
			Conn:         cn,
			SharedBuffer: bytes.NewBuffer(make([]byte, 64000)),
			msgLen:       0,
		}
		conn.Write([]byte{0, 0, 0, 1})
		req.ReadInLengthBuffer()
		c.Assert(req.msgLen, Equals, 1)
	}
	s.RunInProxy(fn)
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
