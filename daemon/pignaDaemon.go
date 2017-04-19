package main

import (
	"flag"
	"github.com/puskin94/pigna/daemon/pignaDaemon"
	"log"
)

const (
	swName    = "goQue"
	swVersion = "0.0.1"
)

func main() {
	var port string

	flag.StringVar(&port, "port", "16789", "serve on this port")
	flag.Parse()

	log.Println("Welcome to " + swName + " v. " + swVersion)
	log.Println("Starting daemon on port", port)
	pignaDaemon.StartServer("0.0.0.0", port)
}
