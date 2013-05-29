package main

import (
	"flag"
	"github.com/mtchavez/roxy/roxy"
	"log"
	"os"
	"runtime"
)

func init() {
	config := flag.String("config", "./roxy/config.toml", "Path to config file")

	flag.Usage = func() {
		log.Printf("Usage %s [OPTIONS] [name ...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	checkConfig(*config)
	roxy.ParseConfig(*config)
	runtime.GOMAXPROCS(runtime.NumCPU())
	poolSize := roxy.Configuration.Doc.GetInt("riak.pool_size", 5)
	roxy.FillPool(poolSize)
	go roxy.StatPoller()
}

func main() {
	roxy.RunProxy()
}

func checkConfig(path string) {
	info, err := os.Lstat(path)
	if err != nil || info.IsDir() {
		log.Println("Unable to find config file at: ", path)
		flag.PrintDefaults()
		os.Exit(2)
	}
}
