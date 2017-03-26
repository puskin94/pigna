package main

import (
	"log"
	"strconv"
	"./pigna"
)

func main() {
	pignaConn, err := pigna.Connect("localhost", "16789")
	if err != nil {
		log.Println(err.Error())
	}

	res, err := pignaConn.CreateQueue("queueTestName")
	if err != nil {
		log.Println(err.Error())
		return
	}
	if res.ResponseType == "error" {
		log.Println(res.ResponseText)
	}

	res, err = pignaConn.ConsumeQueue("queueTestName")
	if err != nil {
		log.Println(err.Error())
		return
	}
	if res.ResponseType == "error" {
		log.Println(res.ResponseText)
	}

	res, err = pignaConn.RemoveConsumer("queueTestName")
	if err != nil {
		log.Println(err.Error())
		return
	}
	if res.ResponseType == "error" {
		log.Println(res.ResponseText)
	}

	for i := 0; i < 100; i++ {
		pignaConn.SendMsg("queueTestName", strconv.Itoa(i))
	}

	res, err = pignaConn.DestroyQueue("queueTestName")
	if err != nil {
		log.Println(err.Error())
		return
	}
	if res.ResponseType == "error" {
		log.Println(res.ResponseText)
	}

	pignaConn.Disconnect()
}
