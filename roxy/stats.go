package roxy

import (
	"github.com/mtchavez/go-statsite/statsite"
	"io"
	"log"
	"strconv"
	"time"
)

func InitStatsite() (client *statsite.Client, err error) {
	return statsite.NewClient("0.0.0.0:8125")
}

func StatPoller() {
	for {
		time.Sleep(10 * time.Second)
		log.Println("WaitSize: ", len(RiakPool.WaitQueue))
		go trackWaitQueueSize()
		go trackTotalClients()
	}
}

func trackTotalClients() {
	if !StatsEnabled {
		return
	}
	retries := 0
Retry:
	msg := &statsite.CountMsg{"roxy.clients.total", strconv.Itoa(TotalClients)}
	client, err := InitStatsite()
	if err != nil {
		goto Retry
	}
	_, err = client.Emit(msg)
	if err != nil && retries <= 3 {
		if err == io.ErrClosedPipe {
			retries++
			InitStatsite()
			goto Retry
		}

		log.Println("Error writing to statsite: ", err)
	}
}

func trackWaitQueueSize() {
	if !StatsEnabled {
		return
	}
	retries := 0
Retry:
	msg := &statsite.CountMsg{"roxy.requests.waiting", strconv.Itoa(len(RiakPool.WaitQueue))}
	client, err := InitStatsite()
	if err != nil {
		goto Retry
	}
	_, err = client.Emit(msg)
	if err != nil && retries <= 3 {
		if err == io.ErrClosedPipe {
			retries++
			InitStatsite()
			goto Retry
		}

		log.Println("Error writing to statsite: ", err)
	}
}

func (req *Request) trackCmdsProcessed() {
	if !StatsEnabled {
		return
	}
	retries := 0
Retry:
	msg := &statsite.CountMsg{"roxy.commands.processed", "1"}
	_, err := req.StatsClient.Emit(msg)
	if err != nil && retries <= 3 {
		if err == io.ErrClosedPipe {
			retries++
			InitStatsite()
			goto Retry
		}
		log.Println("Error writing to statsite: ", err)
	}
}

func (req *Request) trackLatency(startTime, endTime time.Time) {
	if !StatsEnabled {
		return
	}
	ms := float64(endTime.Sub(startTime)) / float64(time.Millisecond)
	time := strconv.FormatFloat(ms, 'E', -1, 64)
	retries := 0
Retry:
	msg := &statsite.TimeMsg{"roxy.write.time", time}
	_, err := req.StatsClient.Emit(msg)
	if err != nil && retries <= 3 {
		if err == io.ErrClosedPipe {
			retries++
			InitStatsite()
			goto Retry
		}
		log.Println("Error writing to statsite: ", err)
	}
}