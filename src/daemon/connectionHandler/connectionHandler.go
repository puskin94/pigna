package connectionHandler

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	// "strconv"
	"sync"
	// "time"
)

type MsgAction struct {
	Action     string  `json:"action"`
	SenderName string  `json:"senderName"`
	Message    Message `json:"message"`
	Queue      Queue   `json:"queue"`
}

type QueueList struct {
	Queues []Queue
}

type Queue struct {
	QueueName          string `json:"queueName"`
	NeedsAck           bool   `json:"needsAck"`
	Consumers          []Client
	UnconsumedMessages []Message
	UnackedMessages    []Message
	MutexCounter       sync.Mutex
	MsgCounter         int
}

type Client struct {
	Connection net.Conn
	Name       string
}

type Message struct {
	Body       string `json:"body"`
	MsgId      int    `json:"msgId"`
	SenderConn net.Conn
	SenderName string
}

type MessageSorter []Message

func (a MessageSorter) Len() int           { return len(a) }
func (a MessageSorter) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a MessageSorter) Less(i, j int) bool { return a[i].MsgId < a[j].MsgId }

var queueList QueueList

func StartServer(host, port string) {
	l, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Println("Error listening:", err.Error())
		os.Exit(1)
	}

	// Close the listener when the application closes.
	defer l.Close()
	for {
		conn, err := l.Accept()

		if err != nil {
			log.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}

		go handleRequest(conn)
	}
}

func (q *QueueList) addQueue(newQueue Queue) []Queue {
	q.Queues = append(q.Queues, newQueue)
	return q.Queues
}

func (q *QueueList) destroyQueue(queueIdx int) []Queue {
	q.Queues[queueIdx] = q.Queues[len(q.Queues)-1]
	q.Queues = q.Queues[:len(q.Queues)-1]
	return q.Queues
}

func (q *Queue) addConsumer(conn net.Conn, senderName string) []Client {
	var c Client
	c.Connection = conn
	c.Name = senderName
	q.Consumers = append(q.Consumers, c)
	return q.Consumers
}

func (q *Queue) addUnconsumedMessage(message Message) []Message {
	q.UnconsumedMessages = append(q.UnconsumedMessages, message)
	return q.UnconsumedMessages
}

func (q *Queue) deleteConsumer(clIdx int) []Client {
	q.Consumers[clIdx] = q.Consumers[len(q.Consumers)-1]
	q.Consumers = q.Consumers[:len(q.Consumers)-1]
	return q.Consumers
}

func handleRequest(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text()
		log.Println("Client sends: " + msg)

		msgAct := new(MsgAction)
		err := json.Unmarshal([]byte(msg), &msgAct)
		if err != nil {
			writeMessageString(conn, "error", "Invalid JSON request. "+err.Error())
			return
		}

		errAction, resType, resText := checkMsgAction(msgAct)
		if errAction {
			writeMessageString(conn, resType, resText)
			return
		}

		if msgAct.Action == "createQueue" {
			actionCreateQueue(conn, *msgAct)
		} else if msgAct.Action == "checkQueueName" {
			_, err := checkQueueName(msgAct.Queue)
			var res bool
			if err != nil {
				res = false
			} else {
				res = true
			}
			writeMessageBool(conn, "success", res)
		} else if msgAct.Action == "consumeQueue" {
			actionConsumeQueue(conn, *msgAct)
		} else if msgAct.Action == "getNamesOfPaired" {
			actionGetNamesOfPaired(conn, *msgAct)
		} else if msgAct.Action == "getQueueNames" {
			actionGetQueueNames(conn, *msgAct)
		} else if msgAct.Action == "getNumOfPaired" {
			actionGetNumberOfPaired(conn, *msgAct)
		} else if msgAct.Action == "getNumOfUnacked" {
			actionGetNumOfUnacked(conn, *msgAct)
		} else if msgAct.Action == "getNumOfUnconsumed" {
			actionGetNumOfUnconsumed(conn, *msgAct)
		} else if msgAct.Action == "sendMsg" {
			actionSendMsg(conn, *msgAct)
		} else if msgAct.Action == "msgAck" {
			actionAckMessage(conn, *msgAct)
		} else if msgAct.Action == "destroyQueue" {
			actionDestroyQueue(conn, *msgAct)
		} else if msgAct.Action == "removeConsumer" {
			actionRemoveConsumer(conn, *msgAct)
		}
	}
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
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessageString(conn, "error", "This queueName does not exists")
		return
	}
	_, err = checkConsumers(conn, queueIdx, msgAct.SenderName)
	if err == nil {
		writeMessageString(conn, "error", "Already consuming this queue")
		return
	}

	// adding the connection to the proper queue `Consumers`
	queueList.Queues[queueIdx].addConsumer(conn, msgAct.SenderName)

	sort.Sort(MessageSorter(queueList.Queues[queueIdx].UnconsumedMessages))
	// clear the queue sending the `UnconsumedMessages`
	for _, _ = range queueList.Queues[queueIdx].UnconsumedMessages {
		broadcastToQueue(queueList.Queues[queueIdx],
			queueList.Queues[queueIdx].UnconsumedMessages[0])
		queueList.Queues[queueIdx].UnconsumedMessages =
			queueList.Queues[queueIdx].UnconsumedMessages[1:]
	}
}

func broadcastToQueue(q Queue, message Message) {
	// send the body to all the Consumers connections
	for idx, _ := range q.Consumers {
		msg := fmt.Sprintf(`{"responseType":"recvMsg", "queueName":"%s", `+
			`"responseTextString": "%s", "senderName": "%s", "msgId": %d,`+
			`"needsAck": %v}`,
			q.QueueName, message.Body, message.SenderName,
			message.MsgId, q.NeedsAck)

		if q.NeedsAck {
			q.UnackedMessages = append(q.UnackedMessages, message)
		}

		sendToClient(q.Consumers[idx].Connection, msg)

	}
}

func checkConsumers(conn net.Conn, queueIdx int, consumerName string) (int, error) {
	for idx, _ := range queueList.Queues[queueIdx].Consumers {
		if conn == queueList.Queues[queueIdx].Consumers[idx].Connection ||
			consumerName == queueList.Queues[queueIdx].Consumers[idx].Name {
			return idx, nil
		}
	}
	return -1, errors.New("No consumer on this queue")
}

func checkQueueName(queue Queue) (int, error) {
	for idx, _ := range queueList.Queues {
		if queue.QueueName == queueList.Queues[idx].QueueName {
			return idx, nil
		}
	}
	return -1, errors.New("No queue with this name")
}

func copyQueueStruct(m *MsgAction, q *Queue) {
	q.QueueName = m.Queue.QueueName
	q.NeedsAck = m.Queue.NeedsAck
}

func checkMsgAction(m *MsgAction) (bool, string, string) {
	var err bool = false
	var resText string = ""

	if isValidAction(m.Action) == false {
		err = true
		resText = "Invalid Action"
	}
	if !err {
		err, _, resText = checkMsgParameters(m)
	}
	return err, "error", resText
}

func checkMsgParameters(m *MsgAction) (bool, string, string) {
	var err bool = false
	var resText string = ""

	// `QueueName` is mandatory for every message type
	if m.Queue.QueueName == "" && m.Action != "getQueueNames" {
		err = true
		resText = "Invalid queueName"
	}

	if m.Action == "sendMsg" {
		if m.Message.Body == "" {
			err = true
			resText = "Missing the 'body' param"
		}
	}

	return err, "error", resText
}

func isValidAction(action string) bool {
	switch action {
	case
		"getNumOfUnconsumed",
		"getNumOfUnacked",
		"getNamesOfPaired",
		"getNumOfPaired",
		"getQueueNames",
		"checkQueueName",
		"sendMsg",
		"msgAck",
		"consumeQueue",
		"removeConsumer",
		"destroyQueue",
		"createQueue":
		return true
	}
	return false
}

func writeMessageString(conn net.Conn, messageType string, message string) {
	msg := fmt.Sprintf(`{"responseType": "%s", "responseTextString": "%s"}`,
		messageType, message)
	sendToClient(conn, msg)
}

func writeMessageInt(conn net.Conn, messageType string, message int) {
	msg := fmt.Sprintf(`{"responseType": "%s", "responseTextInt": %d}`,
		messageType, message)
	sendToClient(conn, msg)
}

func writeMessageBool(conn net.Conn, messageType string, message bool) {
	msg := fmt.Sprintf(`{"responseType": "%s", "responseTextBool": %v}`,
		messageType, message)
	sendToClient(conn, msg)
}

func sendToClient(conn net.Conn, message string) {
	conn.Write([]byte(message + "\n"))
	log.Println(message)
	// time.Sleep(0 * time.Millisecond)
}
