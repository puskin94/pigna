package pignaDaemon

import (
	"net"
	"strings"

	// "github.com/puskin94/pigna"
)

// func actionDistributeQueues(conn net.Conn, msgAct MsgAction) {
// 	// numQueues := len(queueList.Queues)
// 	totNumQueues := 0
// 	numClusterNodes := len(clusterNodes)
//
// 	for hostname, _ := range clusterNodes {
// 		// // act like a normal pigna request
// 		// req := MsgAction{
// 		// 	Action: "getQueueNames",
// 		// }
// 		//
// 		// reqString, _ := json.Marshal(req)
// 		// sendToClient(mainPigna, string(reqString))
// 		pignaConn, err := pigna.Connect(hostname, "")
// 		if err != nil {
// 			log.Println("Host " + hostname + " is unreachable!")
// 			continue
// 		}
// 		num, _ := pignaConn.GetNumberOfQueues()
//
// 		pignaConn.Disconnect()
// 		totNumQueues += num
// 	}
// }

func actionAddClusterNode(conn net.Conn, msgAct MsgAction) {
	for hostname, node := range clusterNodes {
		if node.Connection == conn || hostname == msgAct.Message.Body {
			// writeMessageString(conn, "error", "Already part of this cluster!")
			return
		}
	}

	clusterNodes[msgAct.Message.Body] = ClusterNode {
		Connection: conn,
	}
	// writeMessageString(conn, "success", "Already part of this cluster!")
}

func actionGetNumOfQueues(conn net.Conn, msgAct MsgAction) {
	writeMessageInt(conn, "success", len(queueList.Queues))
}

func actionGetQueueNames(conn net.Conn, msgAct MsgAction) {
	var names []string
	var msg string
	for _, q := range queueList.Queues {
		names = append(names, q.QueueName)
	}

	msg = strings.Join(names, ",")
	writeMessageString(conn, "success", msg)
}

func actionGetNumOfUnacked(conn net.Conn, msgAct MsgAction) {
	isPresent, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}
	writeMessageInt(conn, "success", len(queueList.Queues[msgAct.Queue.QueueName].UnackedMessages))
}

func actionGetNumOfUnconsumed(conn net.Conn, msgAct MsgAction) {
	isPresent, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}
	writeMessageInt(conn, "success", len(queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages))
}

func actionCheckQueueName(conn net.Conn, msgAct MsgAction) {
	isPresent, _ := checkQueueName(msgAct.Queue)
	writeMessageBool(conn, "success", isPresent)
}

func actionAckMessage(conn net.Conn, msgAct MsgAction) {
	isPresent, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}

	if !queueList.Queues[msgAct.Queue.QueueName].NeedsAck {
		return
	}
	for msgIdx, msg := range queueList.Queues[msgAct.Queue.QueueName].UnackedMessages {
		if msg.MsgId == msgAct.Message.MsgId {
			queueList.Queues[msgAct.Queue.QueueName].UnackedMessages =
				append(queueList.Queues[msgAct.Queue.QueueName].UnackedMessages[:msgIdx],
					queueList.Queues[msgAct.Queue.QueueName].UnackedMessages[msgIdx+1:]...)
		}
	}
}

func actionGetNamesOfPaired(conn net.Conn, msgAct MsgAction) {
	isPresent, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}
	var names []string
	var msg string
	for _, client := range queueList.Queues[msgAct.Queue.QueueName].Consumers {
		names = append(names, client.Name)
	}

	msg = strings.Join(names, ",")
	writeMessageString(conn, "success", msg)
}

func actionGetNumberOfPaired(conn net.Conn, msgAct MsgAction) {
	isPresent, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}
	writeMessageInt(conn, "success", len(queueList.Queues[msgAct.Queue.QueueName].Consumers))
}

func actionRemoveConsumer(conn net.Conn, msgAct MsgAction) {
	isPresent, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}
	connIdx, err := checkConsumers(conn, msgAct.Queue.QueueName, msgAct.SenderName)
	if err != nil {
		writeMessageString(conn, "error", "You are not consuming this Queue")
		return
	}

	queueList.Queues[msgAct.Queue.QueueName].deleteConsumer(connIdx)

	writeMessageString(conn, "success", "You are not consuming anymore")
}

func actionDestroyQueue(conn net.Conn, msgAct MsgAction) {
	isPresent, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}
	queueList.destroyQueue(msgAct.Queue.QueueName)

	writeMessageString(conn, "success", "Queue "+msgAct.Queue.QueueName+
		" destroyed")
}

func actionSendMsg(conn net.Conn, msgAct MsgAction) {
	isPresent, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}

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
			broadcastToQueue(*queue,
				msgAct.Message)
		}
	// "loadBalanced" == send messages to connections in RoundRobin mode
	} else if queue.QueueType == "loadBalanced" {
		msg := formatMessage(*queue, msgAct.Message)
		sendToClient(queue.Consumers[queue.LastRRIdx-1].Connection, msg)
		// circolar list
		if queue.LastRRIdx % len(queue.Consumers) == 0 {
			queue.LastRRIdx = 1
		} else {
			queue.LastRRIdx++
		}
	}
}

func actionHasBeenAcked(conn net.Conn, msgAct MsgAction) {
	isPresent, err := checkQueueName(msgAct.Queue)
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

func actionCreateQueue(conn net.Conn, msgAct MsgAction) {
	isPresent, _ := checkQueueName(msgAct.Queue)
	if isPresent {
		writeMessageString(conn, "error", "This queueName already exists")
		return
	}

	var validQueueTypes = map[string]bool{
		"normal": true,
		"loadBalanced": true,
	}

	_, isValid := validQueueTypes[msgAct.Queue.QueueType]
	if !isValid {
		writeMessageString(conn, "error", "Invalid queue type")
		return
	}
	var newQueue Queue
	newQueue.MsgCounter = 0
	newQueue.NeedsAck = false
	newQueue.LastRRIdx = 1
	copyQueueStruct(&msgAct, &newQueue)
	queueList.addQueue(newQueue)
	writeMessageString(conn, "success", "Queue "+msgAct.Queue.QueueName+
		" created successfully")
}

func actionConsumeQueue(conn net.Conn, msgAct MsgAction) {
	var consumerHasChangedSocket bool = false
	isPresent, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}
	consumerIdx, err := checkConsumers(conn, msgAct.Queue.QueueName, msgAct.SenderName)

	if err == nil {
		// update che conn socket if the same consumer has changed it
		if queueList.Queues[msgAct.Queue.QueueName].Consumers[consumerIdx].Connection != conn {
			queueList.Queues[msgAct.Queue.QueueName].Consumers[consumerIdx].Connection = conn
			consumerHasChangedSocket = true
		} else {
			writeMessageString(conn, "error", "Already consuming this queue")
			return
		}
	}

	// adding the connection to the proper queue `Consumers` only if there is a new socket
	if !consumerHasChangedSocket {
		queueList.Queues[msgAct.Queue.QueueName].addConsumer(conn, msgAct.SenderName)
		writeMessageString(conn, "success", "Consuming the queue")
	}

	// clear the queue sending the `UnconsumedMessages`
	for _, _ = range queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages {
		broadcastToQueue(*queueList.Queues[msgAct.Queue.QueueName],
			queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages[0])
		queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages =
			queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages[1:]
	}
}
