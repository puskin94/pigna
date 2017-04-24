package main

import (
	"flag"
	"log"
	"github.com/puskin94/pigna/daemon/pignaDaemon"
)

const (
	swName    = "Pigna"
	swVersion = "0.0.1"
)

func main() {
	var port string
	var debug bool

	flag.BoolVar(&debug, "debug", false, "Display incoming and outcoming messages")
	flag.StringVar(&port, "port", "16789", "serve on this port")
	flag.Parse()

	if debug {
		log.Println("Welcome to " + swName + " v. " + swVersion)
		log.Println("Starting daemon on port", port)
	}
	pignaDaemon.StartServer("0.0.0.0", port, debug)
}
