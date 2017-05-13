package pignaDaemon

import (
	"encoding/base64"
	"encoding/json"
	"github.com/puskin94/pigna"
	"log"
	"net"
	"strings"
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
	clusterNodes[msgAct.Message.Body] = ClusterNode{
		Connection: conn,
		QueueList:  &queueList,
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

		var pignaQueue pigna.Queue = pigna.Queue{
			QueueName: queue.QueueName,
			QueueType: queue.QueueType,
			NeedsAck:  queue.NeedsAck,
			ForwardConn: pigna.PignaConnection{
				Port: queue.ServerQueue.ForwardPort,
			},
			HostOwner: host,
			PortOwner: port,
		}

		queueString, _ := json.Marshal(pignaQueue)
		writeMessageStringEncoded(conn, "success", string(base64.StdEncoding.EncodeToString([]byte(queueString))))
	} else {

		var validQueueTypes = map[string]bool{
			"normal":     true,
			"roundRobin": true,
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

			// get a free port where to listen
			l, _ := net.Listen("tcp", "")
			port := getPort(l.Addr())
			l.Close()

			var pignaQueue pigna.Queue = pigna.Queue{
				QueueName: msgAct.Queue.QueueName,
				QueueType: msgAct.Queue.QueueType,
				NeedsAck:  msgAct.Queue.NeedsAck,
				HostOwner: thisHost,
				PortOwner: thisPort,
				ForwardConn: pigna.PignaConnection{
					Hostname: thisHost,
					Port:     port,
				},
			}

			queueList.Queues[msgAct.Queue.QueueName].ServerQueue.ForwardPort = port
			go createServerQueue(*queueList.Queues[msgAct.Queue.QueueName], port)

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
