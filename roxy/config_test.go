package roxy

import (
	. "launchpad.net/gocheck"
)

func (s *MySuite) TestBadConfigPath(c *C) {
	parse := func() { ParseConfig("invalid/path") }
	c.Assert(parse, PanicMatches, `open.*: no such file or directory`)
}

func (s *MySuite) TestValidConfig(c *C) {
	ParseConfig("./config.toml")

	roxy_ip := Configuration.Doc.GetString("roxy.ip")
	roxy_port := Configuration.Doc.GetInt("roxy.port")
	c.Assert(roxy_ip, Equals, "127.0.0.1")
	c.Assert(roxy_port, Equals, 8088)

	riak_ip := Configuration.Doc.GetString("riak.ip")
	riak_port := Configuration.Doc.GetInt("riak.port")
	pool_size := Configuration.Doc.GetInt("riak.pool_size")
	c.Assert(riak_ip, Equals, "127.0.0.1")
	c.Assert(riak_port, Equals, 8087)
	c.Assert(pool_size, Equals, 5)
}
