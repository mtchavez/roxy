package main

import (
	"./roxy"
	"flag"
	"log"
	"os"
)

func main() {
	config := flag.String("config", "./roxy/config.toml", "Path to config file")

	flag.Usage = func() {
		log.Printf("Usage %s [OPTIONS] [name ...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	checkConfig(*config)
	roxy.Setup(*config)
	roxy.Run()
}

func checkConfig(path string) {
	info, err := os.Lstat(path)
	if err != nil || info.IsDir() {
		log.Println("Unable to find config file at: ", path)
		flag.PrintDefaults()
		os.Exit(2)
	}
}
