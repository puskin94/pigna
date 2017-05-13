package pignaDaemon

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
)

func createServerQueue(queue Queue, port string) {

	// forwardConn, err := net.Dial("tcp", hostname+":"+forwardPort)
	server, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return
	}

	for {
		c, err := server.Accept()

		if err != nil {
			log.Println("Error accepting: ", err.Error())
			return
		}

		go handleQueueRequest(c, port)
	}

}

func actionConsumeQueue(conn net.Conn, msgAct MsgAction) {
	isPresent, clusterNode, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}

	// the queue to Consume is on another pignaDaemon!
	if clusterNode != "" {
		writeMessageString(conn, "error", "This queue is in another daemon")
		return
	}

	consumerIdx, err := checkConsumers(conn, msgAct.Queue.QueueName, msgAct.SenderName)

	var consumerHasChangedSocket bool = false

	if err == nil {
		// update che conn socket if the same consumer has changed it
		if queueList.Queues[msgAct.Queue.QueueName].Consumers[consumerIdx].ForwardConn != conn {
			queueList.Queues[msgAct.Queue.QueueName].Consumers[consumerIdx].ForwardConn = conn
			consumerHasChangedSocket = true
		} else {
			writeMessageString(conn, "error", "Already consuming this queue")
			return
		}
	}

	// adding the connection to the proper queue `Consumers` only if there is a new socket
	if !consumerHasChangedSocket {
		queueList.Queues[msgAct.Queue.QueueName].addConsumer(conn,
			msgAct.SenderName)
	}

	writeMessageString(conn, "success", "")

	// clear the queue sending the `UnconsumedMessages`
	for _, _ = range queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages {
		broadcastToQueue(queueList.Queues[msgAct.Queue.QueueName],
			queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages[0])
		queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages =
			queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages[1:]
	}
}

func handleQueueRequest(conn net.Conn, forwardPort string) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text()
		// log.Println(msg + "\n")

		msgAct := new(MsgAction)
		err := json.Unmarshal([]byte(msg), &msgAct)
		if err != nil {
			writeMessageString(conn, "error", "Invalid JSON request. "+err.Error())
			return
		}

		if msgAct.Action == "sendMsg" {
			actionSendMsg(conn, *msgAct)
		} else if msgAct.Action == "consumeQueue" {
			actionConsumeQueue(conn, *msgAct)
		} else if msgAct.Action == "msgAck" {
			actionAckMessage(conn, *msgAct)
		}
	}
}

func actionAckMessage(conn net.Conn, msgAct MsgAction) {
	isPresent, _, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}

	if !queueList.Queues[msgAct.Queue.QueueName].NeedsAck {
		return
	}
	for msgIdx, msg := range queueList.Queues[msgAct.Queue.QueueName].UnackedMessages {
		if msg.MsgUUID == msgAct.Message.MsgUUID {
			queueList.Queues[msgAct.Queue.QueueName].UnackedMessages =
				append(queueList.Queues[msgAct.Queue.QueueName].UnackedMessages[:msgIdx],
					queueList.Queues[msgAct.Queue.QueueName].UnackedMessages[msgIdx+1:]...)
		}
	}
}

func actionSendMsg(conn net.Conn, msgAct MsgAction) {
	// isPresent, _, err := checkQueueName(msgAct.Queue)
	// if !isPresent {
	// 	writeMessageString(conn, "error", err.Error())
	// 	return
	// }

	msgAct.Message.SenderConn = conn
	msgAct.Message.SenderName = msgAct.SenderName

	queue := queueList.Queues[msgAct.Queue.QueueName]

	queue.MutexCounter.Lock()
	queue.MsgCounter++
	msgAct.Message.MsgId = queue.MsgCounter
	queue.MutexCounter.Unlock()

	// "normal" == send messages to all
	if queue.QueueType == "normal" {
		// if there are no `Consumers`, add to the queue
		if len(queue.Consumers) == 0 {
			queue.addUnconsumedMessage(msgAct.Message)
		} else {
			broadcastToQueue(queue,
				msgAct.Message)
		}
		// "roundRobin" == send messages to connections in RoundRobin mode
	} else if queue.QueueType == "roundRobin" {
		msg := formatMessage(*queue, msgAct.Message)
		sendToClient(queue.Consumers[queue.LastRRIdx-1].ForwardConn, msg)
		// circolar list
		if queue.LastRRIdx%len(queue.Consumers) == 0 {
			queue.LastRRIdx = 1
		} else {
			queue.LastRRIdx++
		}
	}
}

func actionHasBeenAcked(conn net.Conn, msgAct MsgAction) {
	isPresent, _, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}

	for _, q := range queueList.Queues[msgAct.Queue.QueueName].UnackedMessages {
		if q.MsgUUID == msgAct.Message.MsgUUID {
			writeMessageBool(conn, "success", false)
			return
		}
	}
	writeMessageBool(conn, "success", true)
}
