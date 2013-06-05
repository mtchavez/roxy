package main

import (
	"flag"
	"fmt"
	"github.com/mtchavez/roxy/roxy"
	"os"
)

func init() {
	config := flag.String("config", "./roxy/config.toml", "Path to config file")
	version := flag.Bool("v", false, "prints current roxy version")
	flag.Usage = func() {
		fmt.Printf("Usage %s [OPTIONS] [name ...]\n", os.Args[0])
		fmt.Printf("version or v: Prints current roxy version\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if *version {
		fmt.Println(roxy.VERSION)
		os.Exit(0)
	}
	checkConfig(*config)
	roxy.Setup(*config)
}

func main() {
	roxy.RunProxy()
}

func checkConfig(path string) {
	info, err := os.Lstat(path)
	if err != nil || info.IsDir() {
		fmt.Println("Unable to find config file at: ", path)
		flag.PrintDefaults()
		os.Exit(2)
	}
}
