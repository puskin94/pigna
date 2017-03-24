package main


import (
	"log"
	"./connectionHandler"
)

const (
	swName = "goQue"
	swVersion = "0.0.1"
	host = "localhost"
	port = "16789"
)

func main() {
	log.Println("Welcome to " + swName + " v. " + swVersion) 
	log.Println("Starting daemon on port", port )
	connectionHandler.StartServer(host, port)
}
