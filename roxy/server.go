package roxy

import (
	"code.google.com/p/goprotobuf/proto"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
)

// Server holds the connection for the Roxy net.Listener and
// a map of all the active connections
type Server struct {
	ListenerConn net.Listener
	Conns        map[net.Conn]int
}

var RoxyServer = Server{}
var TotalClients = 0
var StatsEnabled = false
var Shutdown = make(chan bool, 5)
var statsClosed = make(chan bool)

func roxyServerString() string {
	roxy_ip := Configuration.Doc.GetString("roxy.ip", "127.0.0.1")
	roxy_port := Configuration.Doc.GetInt("roxy.port", 8088)
	return roxy_ip + ":" + strconv.Itoa(roxy_port)
}

func setupErrorResp() {
	errPb := &RpbErrorResp{
		Errmsg:  []byte("Error talking to Riak"),
		Errcode: proto.Uint32(1),
	}
	data, err := proto.Marshal(errPb)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	ErrorResp = append([]byte{0, 0, 0, byte(len(data) + 1), 0}, data...)
}

// Setup takes a path to a config toml file to initialize Roxy.
// This will parse the config file, set GOMAXPROCS, initialize
// a pool for Riak connections based on config and optionally
// initialize a Statsite connection if turned on in config.
//
// For example:
//
//		roxy.Setup("./my-config.toml")
//
func Setup(configpath string) {
	setupErrorResp()
	ParseConfig(configpath)
	runtime.GOMAXPROCS(runtime.NumCPU())
	poolSize := Configuration.Doc.GetInt("riak.pool_size", 5)
	StatsEnabled = Configuration.Doc.GetBool("statsite.enabled", false)
	FillPool(poolSize)
	if StatsEnabled {
		c, err := InitStatsite()
		if err == nil {
			StatsiteClient = c
		}
		go StatPoller()
	}
}

// RunProxy will use config to set up a listener on the supplied
// ip and port for Roxy. If successful then Roxy will be running and
// listening for incomming connection.
func RunProxy() {
	server_string := roxyServerString()
	listenerConn, netErr := net.Listen("tcp", server_string)
	if netErr != nil {
		log.Println("Error connecting to  ", server_string)
		return
	}

	RoxyServer.Conns = make(map[net.Conn]int, 0)
	RoxyServer.ListenerConn = listenerConn
	RoxyServer.Listen()
}

// Listen will listen to a Server net.Listener until a message
// is sent on the Shutdown channel. When Shutdown is sent any
// active connections will be closed and the listener will be closed.
func (s *Server) Listen() {
	defer func() {
		s.ListenerConn.Close()
		if StatsEnabled {
			<-statsClosed
		}
	}()
	checkForTrapSig()
	for {
		select {
		case <-Shutdown:
			log.Println("Listener Shutdown")
			s.closeConnections()
			return
		default:
			conn, err := s.ListenerConn.Accept()
			if err != nil {
				log.Println("Connection error: ", err)
			} else {
				TotalClients++
				s.Conns[conn] = TotalClients
				go RequestHandler(conn)
			}
		}
	}
}

// Server function to close all active connections to Roxy
func (s *Server) closeConnections() {
	for conn, _ := range s.Conns {
		conn.Close()
		TotalClients--
		delete(s.Conns, conn)
	}
	s.Conns = make(map[net.Conn]int, 0)
}

// Convenience method for a Server to shut itself down
// If statsite is enabled a Shutdown message is sent to it as well.
func (s *Server) Shutdown() {
	Shutdown <- true
	if StatsEnabled {
		Shutdown <- true
	}
}

// Traps signals sent to Roxy so that it can gracefully shut down
// and close active connections.
func checkForTrapSig() {
	// trap signal
	sch := make(chan os.Signal, 10)
	signal.Notify(sch, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT,
		syscall.SIGHUP, syscall.SIGSTOP, syscall.SIGQUIT)
	go func(ch chan os.Signal) {
		select {
		case sig := <-ch:
			log.Print("signal recieved " + sig.String())
			RoxyServer.Shutdown()
		}
	}(sch)
}
