package pignaDaemon

import (
	"net"
	"strings"
	"log"
	"bufio"
	"encoding/json"
	"encoding/base64"
	"github.com/puskin94/pigna"
)


func actionAddClusterNode(conn net.Conn, msgAct MsgAction) {
	for hostname, node := range clusterNodes {
		if node.Connection == conn || hostname == msgAct.Message.Body {
			// writeMessageString(conn, "error", "Already part of this cluster!")
			return
		}
	}

	var queueList QueueList
	queueList.Queues = make(map[string]*Queue)
	clusterNodes[msgAct.Message.Body] = ClusterNode {
		Connection: conn,
		QueueList: &queueList,
	}
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
	isPresent, _, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}
	writeMessageInt(conn, "success", len(queueList.Queues[msgAct.Queue.QueueName].UnackedMessages))
}

func actionGetNumOfUnconsumed(conn net.Conn, msgAct MsgAction) {
	isPresent, _, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}
	writeMessageInt(conn, "success", len(queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages))
}

func actionCheckQueueName(conn net.Conn, msgAct MsgAction) {
	isPresent, _, _ := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageBool(conn, "error", false)
	} else {
		writeMessageBool(conn, "success", true)
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
		if msg.MsgId == msgAct.Message.MsgId {
			queueList.Queues[msgAct.Queue.QueueName].UnackedMessages =
				append(queueList.Queues[msgAct.Queue.QueueName].UnackedMessages[:msgIdx],
					queueList.Queues[msgAct.Queue.QueueName].UnackedMessages[msgIdx+1:]...)
		}
	}
}

func actionGetNamesOfPaired(conn net.Conn, msgAct MsgAction) {
	isPresent, _, err := checkQueueName(msgAct.Queue)
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
	isPresent, _, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}
	writeMessageInt(conn, "success", len(queueList.Queues[msgAct.Queue.QueueName].Consumers))
}

func actionRemoveConsumer(conn net.Conn, msgAct MsgAction) {
	isPresent, _, err := checkQueueName(msgAct.Queue)
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
	isPresent, _, err := checkQueueName(msgAct.Queue)
	if !isPresent {
		writeMessageString(conn, "error", err.Error())
		return
	}
	queueList.destroyQueue(msgAct.Queue.QueueName)

	writeMessageString(conn, "success", "Queue "+msgAct.Queue.QueueName+
		" destroyed")
}

func actionSendMsg(conn net.Conn, msgAct MsgAction) {
	isPresent, _, err := checkQueueName(msgAct.Queue)
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
		sendToClient(queue.Consumers[queue.LastRRIdx-1].ForwardConn, msg)
		// circolar list
		if queue.LastRRIdx % len(queue.Consumers) == 0 {
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

func actionCreateQueue(conn net.Conn, msgAct MsgAction) {
	isPresent, host, _ := checkQueueName(msgAct.Queue)
	// just return the queue
	if isPresent {
		// the queue is here locally
		var queue Queue
		var port string
		if host == "" {
			queue = *queueList.Queues[msgAct.Queue.QueueName]
			host = thisHost
			port = thisPort
		} else {
			queue = *clusterNodes[host].QueueList.Queues[msgAct.Queue.QueueName]
			port = clusterNodes[host].Port
		}

		var pignaQueue pigna.Queue = pigna.Queue {
			QueueName: queue.QueueName,
			QueueType: queue.QueueType,
			NeedsAck: queue.NeedsAck,
			HostOwner: host,
			PortOwner: port,
		}

		queueString, _ := json.Marshal(pignaQueue)
		writeMessageStringEncoded(conn, "success", string(base64.StdEncoding.EncodeToString([]byte(queueString))))
	} else {

		var validQueueTypes = map[string]bool{
			"normal": true,
			"loadBalanced": true,
		}

		_, isValid := validQueueTypes[msgAct.Queue.QueueType]
		if !isValid {
			writeMessageString(conn, "error", "Invalid queue type")
			return
		}

		// if there are available cluster nodes, distribute new Queues
		// just add the new queue to who owns the minimum number
		min := len(queueList.Queues)
		selectedHostname := ""
		for hostname, _ := range clusterNodes {
			numQueues := len(clusterNodes[hostname].QueueList.Queues)
			if numQueues < min {
				selectedHostname = hostname
			}
		}

		var newQueue Queue
		newQueue.MsgCounter = 0
		newQueue.LastRRIdx = 1
		copyQueueStruct(&msgAct, &newQueue)
		// var pignaConn pigna.PignaConnection
		// this local node owns the minimum number of queue
		if selectedHostname == "" {
			queueList.addQueue(newQueue)
			var clientConn net.Conn
			if thisIsANode {
				clientConn = msgAct.Queue.ClientConn
			} else {
				clientConn = conn
			}

			var pignaQueue pigna.Queue = pigna.Queue {
				QueueName: msgAct.Queue.QueueName,
				QueueType: msgAct.Queue.QueueType,
				NeedsAck: msgAct.Queue.NeedsAck,
				HostOwner: thisHost,
				PortOwner: thisPort,
			}

			queueString, _ := json.Marshal(pignaQueue)
			writeMessageStringEncoded(clientConn, "success", string(base64.StdEncoding.EncodeToString([]byte(queueString))))
		} else {
			// update the local cache
			clusterNodes[selectedHostname].QueueList.addQueue(newQueue)
			pignaConn, err := pigna.Connect(selectedHostname,
				clusterNodes[selectedHostname].Port, "")
			if err != nil {
				log.Println("Host " + selectedHostname + " is unreachable!")
				return
			}
			queueStruct := pigna.CreateQueueStruct(msgAct.Queue.QueueName)
			queueStruct.QueueType = msgAct.Queue.QueueType
			queueStruct.NeedsAck = msgAct.Queue.NeedsAck
			queueStruct.ClientConn = conn
			pignaConn.CreateQueue(queueStruct)
		}
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

	// hostname := getHostname(conn)
	forwardPort := msgAct.Message.Body

	// forwardConn, err := net.Dial("tcp", hostname+":"+forwardPort)
	server, err := net.Listen("tcp", ":"+forwardPort)
	if err != nil {
		writeMessageString(conn, "error", "Cannot listen")
	}

	writeMessageString(conn, "success", "")

	for {
		c, err := server.Accept()

		if err != nil {
			log.Println("Error accepting: ", err.Error())
			return
		}

		go handleQueueRequest(c, forwardPort)
	}
}

func handleQueueRequest(conn net.Conn, forwardPort string) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text()

		msgAct := new(MsgAction)
		err := json.Unmarshal([]byte(msg), &msgAct)
		if err != nil {
			writeMessageString(conn, "error", "Invalid JSON request. "+err.Error())
			return
		}
		if msgAct.Action == "addConsumer" {
			addConsumer(conn, *msgAct, forwardPort)
		} else if msgAct.Action == "sendMsg" {
			actionSendMsg(conn, *msgAct)
		}
	}

}

func addConsumer(conn net.Conn, msgAct MsgAction, forwardPort string) {
	var consumerHasChangedSocket bool = false

	consumerIdx, err := checkConsumers(conn, msgAct.Queue.QueueName, msgAct.SenderName)

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
			forwardPort, msgAct.SenderName)
	}

	writeMessageString(conn, "success", "")

	// clear the queue sending the `UnconsumedMessages`
	for _, _ = range queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages {
		broadcastToQueue(*queueList.Queues[msgAct.Queue.QueueName],
			queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages[0])
		queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages =
			queueList.Queues[msgAct.Queue.QueueName].UnconsumedMessages[1:]
	}
}
