package roxy

import (
	"bytes"
	. "launchpad.net/gocheck"
	"net"
)

func (s *MySuite) TestSmallerMsgCheckBufferSize(c *C) {
	buffer := bytes.NewBuffer(make([]byte, 16))
	handler := &ClientHandler{Buff: buffer}
	c.Assert(handler.Buff.Bytes(), HasLen, 16)
	handler.checkBufferSize(8)
	c.Assert(handler.Buff.Bytes(), HasLen, 16)
}

func (s *MySuite) TestLargerMsgCheckBufferSize(c *C) {
	buffer := bytes.NewBuffer(make([]byte, 4))
	handler := &ClientHandler{Buff: buffer}
	c.Assert(cap(handler.Buff.Bytes()), Equals, 4)
	handler.checkBufferSize(8)
	c.Assert(cap(handler.Buff.Bytes())+handler.Buff.Len(), Equals, 30)
}

func (s *MySuite) TestReadInLengthBuffer(c *C) {
	fn := func(ch chan net.Conn) {
		conn, _ := net.Dial("tcp", "127.0.0.1:8089")
		cn := <-ch
		handler := &ClientHandler{
			Conn:   cn,
			Buff:   bytes.NewBuffer(make([]byte, 64000)),
			msgLen: 0,
		}
		conn.Write([]byte{0, 0, 0, 1})
		err := handler.ReadInLengthBuffer()
		c.Assert(err, IsNil)
		c.Assert(handler.msgLen, Equals, 1)
		handler.Conn.Close()
		delete(RoxyServer.Conns, handler.Conn)
		TotalClients--
	}
	s.RunInProxy(fn)
}

func (s *MySuite) TestReadingMessage(c *C) {
	fn := func(ch chan net.Conn) {
		conn, _ := net.Dial("tcp", "127.0.0.1:8089")
		cn := <-ch
		handler := &ClientHandler{
			Conn:   cn,
			Buff:   bytes.NewBuffer(make([]byte, 64000)),
			msgLen: 0,
		}
		conn.Write([]byte{0, 0, 0, 1, 1})
		err := handler.Read()
		c.Assert(err, IsNil)
		c.Assert(handler.msgLen, Equals, 1)
		fullMsg := handler.Buff.Bytes()[:4+handler.msgLen]
		c.Assert(fullMsg, DeepEquals, []byte{0, 0, 0, 1, 1})
		handler.Conn.Close()
		delete(RoxyServer.Conns, handler.Conn)
		TotalClients--
	}
	s.RunInProxy(fn)
}

func (s *MySuite) TestClosingRequest(c *C) {
	RoxyServer = Server{Conns: make(map[net.Conn]int, 0)}
	Setup("./config.toml")
	fn := func(ch chan net.Conn) {
		net.Dial("tcp", "127.0.0.1:8089")
		cn := <-ch
		handler := &ClientHandler{
			Conn:   cn,
			Buff:   bytes.NewBuffer(make([]byte, 64000)),
			msgLen: 0,
		}
		TotalClients++
		total := TotalClients
		RoxyServer.Conns[cn] = TotalClients
		c.Assert(RoxyServer.Conns[handler.Conn], Equals, 1)
		handler.Conn.Close()
		delete(RoxyServer.Conns, handler.Conn)
		TotalClients--
		c.Assert(RoxyServer.Conns[handler.Conn], Equals, 0)
		c.Assert(TotalClients, Equals, total-1)
	}
	s.RunInProxy(fn)
}
