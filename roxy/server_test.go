package roxy

import (
	. "launchpad.net/gocheck"
	"runtime"
)

func (s *MySuite) TestRoxyServerString(c *C) {
	ParseConfig("./config.toml")
	c.Assert(roxyServerString(), Equals, "127.0.0.1:8088")
}

func (s *MySuite) TestBadSetup(c *C) {
	setup := func() { Setup("bad/config") }
	c.Assert(setup, PanicMatches, `open.*: no such file or directory`)
}

func (s *MySuite) TestGoodSetup(c *C) {
	Setup("./config.toml")
	maxProcs := runtime.GOMAXPROCS(0)
	c.Assert(maxProcs, Equals, 8)
	c.Assert(RiakPool.ConnQueue, HasLen, 5)
}
