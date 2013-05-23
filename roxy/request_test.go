package roxy

import (
	. "launchpad.net/gocheck"
)

func (s *MySuite) TestValidRequest(c *C) {
	req := &Request{}

	var in chan []byte
	c.Assert(req.ReadIn, FitsTypeOf, in)

	var quit chan bool
	c.Assert(req.Quit, FitsTypeOf, quit)

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
