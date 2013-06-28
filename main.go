package main

import (
	"flag"
	"fmt"
	"github.com/mtchavez/roxy/roxy"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

var profFile *os.File

func init() {
	config := flag.String("config", "./roxy/config.toml", "Path to config file")
	profile := flag.String("memprof", "", "Name of file to output profiling.")
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
	log.Println(*profile)
	if *profile != "" {
		var err error
		profFile, err = os.Create("roxy.pprof")
		if err != nil {
			log.Println(err.Error())
			os.Exit(1)
		}
	}
	checkConfig(*config)
	roxy.Setup(*config)
}

func main() {
	if profFile != nil {
		go func() {
			for {
				time.Sleep(5 * time.Second)
				pprof.WriteHeapProfile(profFile)
			}
		}()

		defer pprof.WriteHeapProfile(profFile)
		defer profFile.Close()
	}
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
