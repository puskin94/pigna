package main

import (
	"flag"
	"github.com/puskin94/pigna/daemon/pignaDaemon"
	"log"
)

const (
	swName    = "Pigna"
	swVersion = "0.0.8"
)

func main() {
	var port string
	var clusterHost string
	var clusterPort string

	flag.StringVar(&port, "port", "16789", "serve on this port")
	flag.StringVar(&clusterHost, "clusterHost", "", "the main pignaDaemon host")
	flag.StringVar(&clusterPort, "clusterPort", "16789", "the main pignaDaemon port")

	flag.Parse()

	log.Println("Welcome to " + swName + " v. " + swVersion)
	log.Println("Starting daemon on port", port)
	pignaDaemon.StartServer("0.0.0.0", port, clusterHost, clusterPort)
}
