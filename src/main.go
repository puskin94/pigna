package main

import (
	"log"
	"flag"
	"strconv"
	"./pigna"
)

func main() {
	var hostname string
	var port int

	flag.StringVar(&hostname, "hostname", "localhost", "connect to this hostname")
	flag.IntVar(&port, "port", 16789, "connect to this port")
	flag.Parse()

	pignaConn, err := pigna.Connect(hostname, strconv.Itoa(port), "config.json")
	if err != nil {
		log.Println(err.Error())
		return
	}

	// check if the queue exists. If not, create it
	res, err := pignaConn.CheckQueueName("queueTestName")
	if err != nil { log.Println(err.Error()); return }
	if res.ResponseText == "false" {
		res, err := pignaConn.CreateQueue("queueTestName")
		if err != nil { log.Println(err.Error()); return }
		if res.ResponseType == "error" {
			log.Println(res.ResponseText)
		}
	}

	pignaConn.ConsumeQueue("queueTestName", hello)

	res, err = pignaConn.GetNumberOfPaired("queueTestName")
	if err != nil { log.Println(err.Error()); return }
	log.Println(res.ResponseText)

	// // // publish random messages
	// for i := 0; i < 10; i++ {
	// 	pignaConn.SendMsg("queueTestName", strconv.Itoa(i))
	// }

	// res, err = pignaConn.RemoveConsumer("queueTestName")
	// if err != nil {
	// 	log.Println(err.Error())
	// 	return
	// }
	// if res.ResponseType == "error" {
	// 	log.Println(res.ResponseText)
	// }

	// res, err = pignaConn.DestroyQueue("queueTestName")
	// if err != nil {
	// 	log.Println(err.Error())
	// 	return
	// }
	// if res.ResponseType == "error" {
	// 	log.Println(res.ResponseText)
	// }

	pignaConn.Disconnect()
}

func hello(msg pigna.Response) {
	log.Println(msg.SenderName + " -> " + msg.QueueName + " -> " + msg.ResponseText)
}
