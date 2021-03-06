package roxy

import (
	"github.com/mtchavez/go-statsite/statsite"
	"log"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var StatsiteClient *statsite.Client
var StatsMux *sync.Mutex
var memStats runtime.MemStats

// Initializes a statsite client based on the toml config file
// Returns a statsite.Client and an error
func InitStatsite() (client *statsite.Client, err error) {
	ip := Configuration.Doc.GetString("statsite.ip", "0.0.0.0")
	port := Configuration.Doc.GetInt("statsite.port", 8125)
	addr := ip + ":" + strconv.Itoa(port)
	client, err = statsite.NewClient(addr)
	StatsMux = &sync.Mutex{}
	if err != nil {
		log.Println("Error connecting to statsite")
	}
	return
}

// A statsite poller to track stats every 10 seconds.
// Currently tracks the size of the WaitQueue on RiakPool,
// and the total clients connected to Roxy
//
// StatPoller can be shutdown by sending true to the Shutdown channel
func StatPoller() {
	for {
		select {
		case <-Shutdown:
			statsClosed <- true
			return
		default:
			time.Sleep(10 * time.Second)
			// TODO: Export runtime.MemStats.PauseTotalNs
			runtime.GC()
			// runtime.ReadMemStats(&memStats)
			trackWaitQueueSize()
			trackTotalClients()
			trackTotalBgProcesses()
		}
	}
}

func trackTotalClients() {
	if !StatsEnabled {
		return
	}
	RoxyServer.Lock()
	msg := &statsite.CountMsg{"roxy.clients.total", strconv.Itoa(TotalClients)}
	RoxyServer.Unlock()
	StatsMux.Lock()
	StatsiteClient.Emit(msg)
	StatsMux.Unlock()
}

func trackWaitQueueSize() {
	if !StatsEnabled {
		return
	}
	RiakPool.Lock()
	msg := &statsite.CountMsg{"roxy.requests.waiting", strconv.Itoa(len(RiakPool.WaitQueue))}
	RiakPool.Unlock()
	StatsMux.Lock()
	StatsiteClient.Emit(msg)
	StatsMux.Unlock()
}

func (req *Request) trackCountForKey(key string) {
	if !StatsEnabled {
		return
	}
	msg := &statsite.CountMsg{key, "1"}
	StatsMux.Lock()
	StatsiteClient.Emit(msg)
	StatsMux.Unlock()
}

func (req *Request) trackTiming(startTime, endTime time.Time, key string) {
	if !StatsEnabled {
		return
	}
	ms := float64(endTime.Sub(startTime)) / float64(time.Millisecond)
	time := strconv.FormatFloat(ms, 'E', -1, 64)
	msg := &statsite.TimeMsg{key, time}
	StatsMux.Lock()
	StatsiteClient.Emit(msg)
	StatsMux.Unlock()
}

func trackTotalBgProcesses() {
	if !StatsEnabled {
		return
	}
	msg := &statsite.CountMsg{"roxy.bgprocs.total", strconv.Itoa(BgHandler.GetTotal())}
	StatsMux.Lock()
	StatsiteClient.Emit(msg)
	StatsMux.Unlock()
}
