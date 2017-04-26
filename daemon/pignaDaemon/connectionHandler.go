package pignaDaemon

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
)

/*
~21s 1000000 with ack
~14s 1000000 without ack
*/

type MsgAction struct {
	Action     string  `json:"action"`
	SenderName string  `json:"senderName"`
	Message    Message `json:"message"`
	Queue      Queue   `json:"queue"`
}

type QueueList struct {
	Queues map[string]*Queue
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
	Body        string `json:"body"`
	MsgId       int    `json:"msgId"`
	MsgUUID     string `json:"UUID"`
	IsAChunk    bool   `json:"isAChunk"`
	NChunk      int    `json:"nChunk"`
	TotalChunks int    `json:"totalChunks"`
	SenderName  string
	SenderConn  net.Conn
}

var validActions = map[string]func(net.Conn, MsgAction) {
	"getNumOfPaired": actionGetNumberOfPaired,
	"createQueue": actionCreateQueue,
	"checkQueueName": actionCheckQueueName,
	"consumeQueue": actionConsumeQueue,
	"getNamesOfPaired": actionGetNamesOfPaired,
	"getQueueNames": actionGetQueueNames,
	"getNumOfUnacked": actionGetNumOfUnacked,
	"getNumOfUnconsumed": actionGetNumOfUnconsumed,
	"sendMsg": actionSendMsg,
	"msgAck": actionAckMessage,
	"hasBeenAcked": actionHasBeenAcked,
	"destroyQueue": actionDestroyQueue,
	"removeConsumer": actionRemoveConsumer,
}

var queueList QueueList
var debug bool

func StartServer(host, port string, isDebug bool) {
	l, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Println("Error listening:", err.Error())
		os.Exit(1)
	}

	debug = isDebug
	queueList.Queues = make(map[string]*Queue)

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

func handleRequest(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text()
		if debug {
			log.Println("Client sends: " + msg)
		}

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

		validActions[msgAct.Action](conn, *msgAct)
	}
}

func (q *QueueList) addQueue(newQueue Queue) map[string]*Queue {
	q.Queues[newQueue.QueueName] = &newQueue
	return q.Queues
}

func (q *QueueList) destroyQueue(queueName string) map[string]*Queue {
	delete(q.Queues, queueName)
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

// XXX: if a message is chunked... wait an ack before send the next chunk?
func broadcastToQueue(q Queue, message Message) {
	// send the body to all the Consumers connections
	for idx, _ := range q.Consumers {
		msg := fmt.Sprintf(`{"responseType":"recvMsg", "queueName":"%s", `+
			`"responseTextString": "%s", "senderName": "%s", "msgId": %d,`+
			`"needsAck": %v, "isAChunk": %v, "nChunk": %d, "totalChunks": %d}`,
			q.QueueName, message.Body, message.SenderName,
			message.MsgId, q.NeedsAck, message.IsAChunk, message.NChunk,
			message.TotalChunks)

		if q.NeedsAck {
			q.UnackedMessages = append(q.UnackedMessages, message)
		}
		sendToClient(q.Consumers[idx].Connection, msg)
	}
}

func checkConsumers(conn net.Conn, queueName string, consumerName string) (int, error) {
	for idx, _ := range queueList.Queues[queueName].Consumers {
		if conn == queueList.Queues[queueName].Consumers[idx].Connection ||
			consumerName == queueList.Queues[queueName].Consumers[idx].Name {
			return idx, nil
		}
	}
	return -1, errors.New("No consumer on this queue")
}

func checkQueueName(queue Queue) (bool, error) {
	_, isPresent := queueList.Queues[queue.QueueName]
	if !isPresent {
		return false, errors.New("No queue with this name")
	}
	return true, nil
}

func copyQueueStruct(m *MsgAction, q *Queue) {
	q.QueueName = m.Queue.QueueName
	q.NeedsAck = m.Queue.NeedsAck
}

func checkMsgAction(m *MsgAction) (bool, string, string) {
	var err bool = false
	var resText string = ""

	_, isPresent := validActions[m.Action]
	if !isPresent {
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
	if debug {
		log.Println(message)
	}
}
