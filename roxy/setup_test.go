package roxy

import (
	. "launchpad.net/gocheck"
	"net"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}
type wrappedTest func(chan net.Conn)

var _ = Suite(&MySuite{})

func (s *MySuite) RunInProxy(fn wrappedTest) {
	ch := make(chan net.Conn, 1)
	cl := make(chan bool)
	listener := s.ProxyServer(ch, cl)
	fn(ch)
	listener.Close()
	cl <- true
}

func (s *MySuite) ProxyServer(ch chan net.Conn, close chan bool) net.Listener {
	serverString := "127.0.0.1:8089"
	listenerConn, netErr := net.Listen("tcp", serverString)
	if netErr != nil {
		return listenerConn
	}
	go func() {
		for {
			select {
			case <-close:
				return
			default:
				conn, err := listenerConn.Accept()
				if err != nil {
					break
				} else {
					ch <- conn
				}
			}
		}
	}()
	return listenerConn
}
