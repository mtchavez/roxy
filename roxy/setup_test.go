package roxy

import (
	. "launchpad.net/gocheck"
	"net"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) RunInProxy(fn func(net.Listener, chan net.Conn, chan bool)) {
	ch := make(chan net.Conn, 1)
	close := make(chan bool, 1)
	listener := s.ProxyServer(ch, close)
	fn(listener, ch, close)
	listener.Close()
	close <- true
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
