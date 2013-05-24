package roxy

import (
	"github.com/mtchavez/go-statsite/statsite"
	"io"
	"log"
)

func InitStatsite() (client *statsite.Client, err error) {
	return statsite.NewClient("0.0.0.0:8125")
}

func (req *Request) trackNewClient() {
	if !req.statsEnabled {
		return
	}
	retries := 0
Retry:
	msg := &statsite.CountMsg{"roxy.clients.new", "1"}
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

func (req *Request) trackCmdsProcessed() {
	if !req.statsEnabled {
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

func (req *Request) trackLatency(time string) {
	if !req.statsEnabled {
		return
	}
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
