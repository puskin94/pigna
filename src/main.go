package main

import (
	"log"
	"flag"
	"strconv"
	// "os/exec"
	"./pigna"
)

func main() {
	var hostname string
	var cmd string
	var port int
	var ack bool

	flag.StringVar(&hostname, "hostname", "localhost", "connect to this hostname")
	flag.StringVar(&cmd, "cmd", "ls", "define the command")
	flag.IntVar(&port, "port", 16789, "connect to this port")
	flag.BoolVar(&ack, "ack", false, "needs ack")

	flag.Parse()

	pignaConn, err := pigna.Connect(hostname, strconv.Itoa(port), "config.json")
	if err != nil {
		log.Println(err.Error())
		return
	}

	// check if the queue exists. If not, create it
	exists, err := pignaConn.CheckQueueName("sendCmd")
	if err != nil { log.Println(err.Error()); return }
	if !exists {
		res, err := pignaConn.CreateQueue("sendCmd", ack)
		if err != nil { log.Println(err.Error()); return }
		if res.ResponseType == "error" {
			log.Println(string(res.ResponseTextString[:]))
		}
	}

	// check if the queue exists. If not, create it
	exists, err = pignaConn.CheckQueueName("cmdRes")
	if err != nil { log.Println(err.Error()); return }
	if !exists {
		res, err := pignaConn.CreateQueue("cmdRes", ack)
		if err != nil { log.Println(err.Error()); return }
		if res.ResponseType == "error" {
			log.Println(string(res.ResponseTextString[:]))
		}
	}

	pignaConn.ConsumeQueue("sendCmd", msgHandler)
	pignaConn.Disconnect()
}

func msgHandler(pignaConn pigna.PignaConnection, msg pigna.Response) {
	log.Println(msg.MsgId)
}
