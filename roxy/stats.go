package roxy

import (
	"github.com/mtchavez/go-statsite/statsite"
	"log"
)

var StatsClient *statsite.Client

func InitStatsite() {
	client, err := statsite.NewClient("0.0.0.0:8125")
	if err != nil {
		return
	}
	StatsClient = client
}

func trackNewClient() {
	retries := 0
Retry:
	msg := &statsite.CountMsg{"roxy.clients.new", "1"}
	_, err := StatsClient.Emit(msg)
	if err != nil && retries <= 3 {
		retries++
		InitStatsite()
		goto Retry
		log.Println("Error writing to statsite: ", err)
	}
}

func trackCmdsProcessed() {
	retries := 0
Retry:
	msg := &statsite.CountMsg{"roxy.commands.processed", "1"}
	_, err := StatsClient.Emit(msg)
	if err != nil && retries <= 3 {
		retries++
		InitStatsite()
		goto Retry
		log.Println("Error writing to statsite: ", err)
	}
}
