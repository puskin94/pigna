package main

import (
	"log"
	"flag"
	"strconv"
	"os/exec"
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
	exists, err := pignaConn.CheckQueueName("queueTestName")
	if err != nil { log.Println(err.Error()); return }
	if !exists {
		res, err := pignaConn.CreateQueue("queueTestName", true)
		if err != nil { log.Println(err.Error()); return }
		if res.ResponseType == "error" {
			log.Println(res.ResponseTextString)
		}
	}
	// names, err := pignaConn.GetNamesOfPaired("queueTestName")
	// if err != nil { log.Println(err.Error()); return }
	// log.Println("Names of consumers:\t", names)
	//
	// names, err = pignaConn.GetQueueNames()
	// if err != nil { log.Println(err.Error()); return }
	// log.Println("Queue Names:\t", names)


	// pignaConn.ConsumeQueue("queueTestName", msgHandler)
	//
	// num, err := pignaConn.GetNumberOfPaired("queueTestName")
	// if err != nil { log.Println(err.Error()); return }
	// log.Println("Number of consumers:\t", num)

	// publish random messages
	// for i := 0; i < 10; i++ {
		pignaConn.SendMsg("queueTestName", "ls -la")
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

func msgHandler(msg pigna.Response) {

	// log.Println(msg.ResponseType)
	// log.Println(msg.ResponseTextString)
	// log.Println(msg.ResponseTextInt)
	// log.Println(msg.ResponseTextBool)
	// log.Println(msg.SenderName)
	// log.Println(msg.QueueName)
	// log.Println(msg.MsgId)
	// log.Println(msg.NeedsAck)
	// log.Println("\n")
	log.Println("Going to execute:\t" + msg.ResponseTextString)
	out, err := exec.Command("sh","-c", msg.ResponseTextString).Output()
	log.Println(string(out[:]))
	if err != nil {
		log.Println(err)
	}


}
