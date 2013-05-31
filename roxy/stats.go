package roxy

import (
	"github.com/mtchavez/go-statsite/statsite"
	"log"
	"strconv"
	"time"
)

var StatsiteClient *statsite.Client

func InitStatsite() (client *statsite.Client, err error) {
	ip := Configuration.Doc.GetString("statsite.ip", "0.0.0.0")
	port := Configuration.Doc.GetInt("statsite.port", 8125)
	addr := ip + ":" + strconv.Itoa(port)
	client, err = statsite.NewClient(addr)
	if err != nil {
		log.Println("Error connecting to statsite")
	}
	return
}

func StatPoller() {
	for {
		select {
		case <-Shutdown:
			statsClosed <- true
			return
		default:
			time.Sleep(10 * time.Second)
			trackWaitQueueSize()
			trackTotalClients()
		}
	}
}

func trackTotalClients() {
	if !StatsEnabled {
		return
	}
	msg := &statsite.CountMsg{"roxy.clients.total", strconv.Itoa(TotalClients)}
	StatsiteClient.Emit(msg)
}

func trackWaitQueueSize() {
	if !StatsEnabled {
		return
	}
	msg := &statsite.CountMsg{"roxy.requests.waiting", strconv.Itoa(len(RiakPool.WaitQueue))}
	StatsiteClient.Emit(msg)
}

func (req *Request) trackCmdsProcessed() {
	if !StatsEnabled {
		return
	}
	msg := &statsite.CountMsg{"roxy.commands.processed", "1"}
	req.StatsClient.Emit(msg)
}

func (req *Request) trackLatency(startTime, endTime time.Time) {
	if !StatsEnabled {
		return
	}
	ms := float64(endTime.Sub(startTime)) / float64(time.Millisecond)
	time := strconv.FormatFloat(ms, 'E', -1, 64)
	msg := &statsite.TimeMsg{"roxy.write.time", time}
	req.StatsClient.Emit(msg)
}
