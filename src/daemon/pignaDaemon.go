package main

import (
	"./connectionHandler"
	"flag"
	"log"
	// "./webServer"
)

const (
	swName    = "goQue"
	swVersion = "0.0.1"
)

func main() {
	var host string
	var port string

	flag.StringVar(&host, "hostname", "localhost", "serve on this ip")
	flag.StringVar(&port, "port", "16789", "serve on this port")
	flag.Parse()

	log.Println("Welcome to " + swName + " v. " + swVersion)
	log.Println("Starting daemon on host", host+":"+port)
	connectionHandler.StartServer(host, port)
	// webServer.StartServer()
}
