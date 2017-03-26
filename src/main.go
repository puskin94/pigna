package main

import (
	"log"
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

	pignaConn.Disconnect()
}
