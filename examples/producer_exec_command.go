package main

import (
	"log"
	"flag"
	"./pigna"
)

func main() {
	var hostname string
	var cmd string

	flag.StringVar(&hostname, "hostname", "localhost", "connect to this hostname")
	flag.StringVar(&cmd, "cmd", "", "define the command")
	flag.Parse()

	pignaConn, err := pigna.Connect(hostname, "16789", "config.json")
	if err != nil {
		log.Println(err.Error())
		return
	}

	// check if the queue exists. If not, create it
	exists, err := pignaConn.CheckQueueName("sendCmd")
	if err != nil { log.Println(err.Error()); return }
	if !exists {
		res, err := pignaConn.CreateQueue("sendCmd", false)
		if err != nil { log.Println(err.Error()); return }
		if res.ResponseType == "error" {
			log.Println(string(res.ResponseTextString[:]))
		}
	}

	// check if the queue exists. If not, create it
	exists, err = pignaConn.CheckQueueName("cmdRes")
	if err != nil { log.Println(err.Error()); return }
	if !exists {
		res, err := pignaConn.CreateQueue("cmdRes", false)
		if err != nil { log.Println(err.Error()); return }
		if res.ResponseType == "error" {
			log.Println(string(res.ResponseTextString[:]))
		}
	}

	pignaConn.ConsumeQueue("cmdRes", msgHandler)
	pignaConn.SendMsg("sendCmd", cmd)
	pignaConn.Disconnect()
}

func msgHandler(pignaConn pigna.PignaConnection, msg pigna.Response) {
	log.Println(msg.SenderName + " ->\n" + msg.ResponseTextString)
}
