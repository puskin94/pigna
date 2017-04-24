package pignaDaemon

import (
	"net"
	"strings"
)

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
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessageString(conn, "error", "This queueName does not exists")
		return
	}
	writeMessageInt(conn, "success", len(queueList.Queues[queueIdx].UnackedMessages))
}

func actionGetNumOfUnconsumed(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessageString(conn, "error", "This queueName does not exists")
		return
	}
	writeMessageInt(conn, "success", len(queueList.Queues[queueIdx].UnconsumedMessages))
}

func actionAckMessage(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessageString(conn, "error", "This queueName does not exists")
		return
	}

	if !queueList.Queues[queueIdx].NeedsAck {
		return
	}
	for msgIdx, msg := range queueList.Queues[queueIdx].UnackedMessages {
		if msg.MsgId == msgAct.Message.MsgId {
			queueList.Queues[queueIdx].UnackedMessages =
				append(queueList.Queues[queueIdx].UnackedMessages[:msgIdx],
					queueList.Queues[queueIdx].UnackedMessages[msgIdx+1:]...)
		}
	}
}

func actionGetNamesOfPaired(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessageString(conn, "error", "This queueName does not exists")
		return
	}
	var names []string
	var msg string
	for _, client := range queueList.Queues[queueIdx].Consumers {
		names = append(names, client.Name)
	}

	msg = strings.Join(names, ",")
	writeMessageString(conn, "success", msg)
}

func actionGetNumberOfPaired(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessageString(conn, "error", "This queueName does not exists")
		return
	}
	writeMessageInt(conn, "success", len(queueList.Queues[queueIdx].Consumers))
}

func actionRemoveConsumer(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessageString(conn, "error", "This queueName does not exists")
		return
	}
	connIdx, err := checkConsumers(conn, queueIdx, msgAct.SenderName)
	if err != nil {
		writeMessageString(conn, "error", "You are not consuming this Queue")
		return
	}

	queueList.Queues[queueIdx].deleteConsumer(connIdx)

	writeMessageString(conn, "success", "You are not consuming anymore")
}

func actionDestroyQueue(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessageString(conn, "error", "This queueName does not exists")
		return
	}
	queueList.destroyQueue(queueIdx)

	writeMessageString(conn, "success", "Queue "+msgAct.Queue.QueueName+
		" destroyed")

}

func actionSendMsg(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessageString(conn, "error", "This queueName does not exists")
		return
	}

	msgAct.Message.SenderConn = conn
	msgAct.Message.SenderName = msgAct.SenderName

	queueList.Queues[queueIdx].MutexCounter.Lock()
	queueList.Queues[queueIdx].MsgCounter++
	msgAct.Message.MsgId = queueList.Queues[queueIdx].MsgCounter
	queueList.Queues[queueIdx].MutexCounter.Unlock()

	// if there are no `Consumers`, add to the queue
	if len(queueList.Queues[queueIdx].Consumers) == 0 {
		queueList.Queues[queueIdx].addUnconsumedMessage(msgAct.Message)
	} else {
		broadcastToQueue(queueList.Queues[queueIdx],
			msgAct.Message)
	}
}

func actionHasBeenAcked(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessageString(conn, "error", "This queueName does not exists")
		return
	}
	for _, q := range queueList.Queues[queueIdx].UnackedMessages {
		if q.MsgUUID == msgAct.Message.MsgUUID {
			writeMessageBool(conn, "success", false)
			return
		}
	}
	writeMessageBool(conn, "success", true)
}

func actionCreateQueue(conn net.Conn, msgAct MsgAction) {
	_, err := checkQueueName(msgAct.Queue)
	if err == nil {
		writeMessageString(conn, "error", "This queueName already exists")
		return
	}
	var newQueue Queue
	newQueue.MsgCounter = 0
	newQueue.NeedsAck = false
	copyQueueStruct(&msgAct, &newQueue)
	queueList.addQueue(newQueue)
	writeMessageString(conn, "success", "Queue "+msgAct.Queue.QueueName+
		" created successfully")
}

func actionConsumeQueue(conn net.Conn, msgAct MsgAction) {
	var consumerHasChangedSocket bool = false
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessageString(conn, "error", "This queueName does not exists")
		return
	}
	consumerIdx, err := checkConsumers(conn, queueIdx, msgAct.SenderName)

	if err == nil {
		// update che conn socket if the same consumer has changed it
		if queueList.Queues[queueIdx].Consumers[consumerIdx].Connection != conn {
			queueList.Queues[queueIdx].Consumers[consumerIdx].Connection = conn
			consumerHasChangedSocket = true
		} else {
			writeMessageString(conn, "error", "Already consuming this queue")
			return
		}
	}

	// adding the connection to the proper queue `Consumers` only if there is a new socket
	if !consumerHasChangedSocket {
		queueList.Queues[queueIdx].addConsumer(conn, msgAct.SenderName)
		writeMessageString(conn, "success", "Consuming the queue")
	}

	// clear the queue sending the `UnconsumedMessages`
	for _, _ = range queueList.Queues[queueIdx].UnconsumedMessages {
		broadcastToQueue(queueList.Queues[queueIdx],
			queueList.Queues[queueIdx].UnconsumedMessages[0])
		queueList.Queues[queueIdx].UnconsumedMessages =
			queueList.Queues[queueIdx].UnconsumedMessages[1:]
	}
}
