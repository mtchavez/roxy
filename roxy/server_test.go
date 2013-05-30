package roxy

import (
	. "launchpad.net/gocheck"
	"time"
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
	c.Assert(RiakPool.ConnQueue, HasLen, 5)
}

func (s *MySuite) TestShutdown(c *C) {
	c.SucceedNow()
	Setup("./config.toml")
	go RunProxy()
	time.Sleep(500 * time.Millisecond)
	RoxyServer.Shutdown()
}
