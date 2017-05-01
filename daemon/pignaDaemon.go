package main

import (
	"flag"
	"github.com/puskin94/pigna/daemon/pignaDaemon"
	"log"
)

const (
	swName    = "Pigna"
	swVersion = "0.0.1"
)

func main() {
	var port string
	var hostname string
	var clusterHost string

	flag.StringVar(&port, "port", "16789", "serve on this port")
	flag.StringVar(&hostname, "hostname", "", "this hostname host:port")
	flag.StringVar(&clusterHost, "clusterHost", "", "the main pignaDaemon host:port")

	flag.Parse()

	if hostname == "" {
		log.Println("Please provide a valid --hostname<:port> or pignaDaemon"+
			" will be never be reachable from the outside")
		return
	}

	log.Println("Welcome to " + swName + " v. " + swVersion)
	log.Println("Starting daemon on port", port)
	pignaDaemon.StartServer("0.0.0.0", port, clusterHost, hostname)
}
