package connectionHandler

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
	"sort"
	"sync"
	"errors"
	"strconv"
	"time"
)

type MsgAction struct {
	Action  string  `json:"action"`
	Message Message `json:"message"`
	Queue   Queue   `json:"queue"`
	SenderName string `json:"senderName"`
}

type QueueList struct {
	Queues []Queue
}

type Queue struct {
	QueueName          string `json:"queueName"`
	Consumers          []Client
	UnconsumedMessages []Message
	MutexCounter       sync.Mutex
	MsgCounter         int
}

type Client struct {
	Connection	net.Conn
	Name		string
}

type Message struct {
	Body		string `json:"body"`
	SenderConn	net.Conn
	SenderName	string
	MsgId     	int
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
		// log.Println("Client sends: " + msg)

		msgAct := new(MsgAction)
		err := json.Unmarshal([]byte(msg), &msgAct)
		if err != nil {
			writeMessage(conn, "error", "Invalid JSON request")
			return
		}

		errAction, resType, resText := checkMsgAction(msgAct)
		if errAction { writeMessage(conn, resType, resText); return }

		if msgAct.Action == "createQueue" {
			actionCreateQueue(conn, *msgAct)
		} else if msgAct.Action == "checkQueueName" {
			_, err := checkQueueName(msgAct.Queue)
			var res string
			if err != nil {
				res = "false"
			} else {
				res = "true"
			}
			writeMessage(conn, "success", res)
		} else if msgAct.Action == "consumeQueue" {
			actionConsumeQueue(conn, *msgAct)
		} else if msgAct.Action == "getNumberOfPaired" {
			actionGetNumberOfPaired(conn, *msgAct)
		} else if msgAct.Action == "sendMsg" {
			actionSendMsg(conn, *msgAct)
		} else if msgAct.Action == "destroyQueue" {
			actionDestroyQueue(conn, *msgAct)
		} else if msgAct.Action == "removeConsumer" {
			actionRemoveConsumer(conn, *msgAct)
		}
	}
}

func actionGetNumberOfPaired(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessage(conn, "error", "This queueName does not exists")
		return
	}
	writeMessage(conn, "success", strconv.Itoa(len(queueList.Queues[queueIdx].Consumers)))
}

func actionRemoveConsumer(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessage(conn, "error", "This queueName does not exists")
		return
	}
	connIdx, err := checkConsumers(conn, queueIdx)
	if err != nil {
		writeMessage(conn, "error", "You are not consuming this Queue")
		return
	}

	queueList.Queues[queueIdx].deleteConsumer(connIdx)

	writeMessage(conn, "success", "You are not consuming anymore")
}

func actionDestroyQueue(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessage(conn, "error", "This queueName does not exists")
		return
	}
	queueList.destroyQueue(queueIdx)

	writeMessage(conn, "success", "Queue " + msgAct.Queue.QueueName+
		" destroyed")

}

func actionSendMsg(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessage(conn, "error", "This queueName does not exists")
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
			msgAct.Message, conn)
	}
}

func actionCreateQueue(conn net.Conn, msgAct MsgAction) {
	_, err := checkQueueName(msgAct.Queue)
	if err == nil {
		writeMessage(conn, "error", "This queueName already exists")
		return
	}
	var newQueue Queue
	newQueue.MsgCounter = 0
	copyQueueStruct(&msgAct, &newQueue)
	queueList.addQueue(newQueue)
	writeMessage(conn, "success", "Queue "+msgAct.Queue.QueueName+
		" created successfully")
}

func actionConsumeQueue(conn net.Conn, msgAct MsgAction) {
	queueIdx, err := checkQueueName(msgAct.Queue)
	if err != nil {
		writeMessage(conn, "error", "This queueName does not exists")
		return
	}
	_, err = checkConsumers(conn, queueIdx)
	if err == nil {
		writeMessage(conn, "error", "Already consuming this queue")
		return
	}

	// adding the connection to the proper queue `Consumers`
	queueList.Queues[queueIdx].addConsumer(conn, msgAct.SenderName)

	sort.Sort(MessageSorter(queueList.Queues[queueIdx].UnconsumedMessages))
	// clear the queue sending the `UnconsumedMessages`
	for _, _ = range queueList.Queues[queueIdx].UnconsumedMessages {
		// if len(queueList.Queues[queueIdx].UnconsumedMessages) == 0 { return }

		// // do not send if the sender is the only one `Consumers`
		// if unconsumed[msgIdx].SenderConn == conn &&
		// 	len(queueList.Queues[queueIdx].Consumers) == 1 {
		// 	continue
		// }

		broadcastToQueue(queueList.Queues[queueIdx],
			queueList.Queues[queueIdx].UnconsumedMessages[0], conn)
		queueList.Queues[queueIdx].UnconsumedMessages =
			queueList.Queues[queueIdx].UnconsumedMessages[1:]
	}
}

func broadcastToQueue(q Queue, message Message, sender net.Conn) {
	// send the body to all the Consumers connections
	for idx, _ := range q.Consumers {
		// if message.SenderConn == sender {
		// 	continue
		// }
		msg := `{"responseType":"recvMsg", "queueName":"` + q.QueueName +
			`","responseText": "` + message.Body + `", "senderName": "` +
			message.SenderName + `"}`
		sendToClient(q.Consumers[idx].Connection, msg)
	}
}

func checkConsumers(conn net.Conn, queueIdx int) (int, error) {
	for idx, _ := range queueList.Queues[queueIdx].Consumers {
		if conn == queueList.Queues[queueIdx].Consumers[idx].Connection {
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
	if m.Queue.QueueName == "" {
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
		"getNumberOfPaired",
		"checkQueueName",
		"sendMsg",
		"consumeQueue",
		"removeConsumer",
		"destroyQueue",
		"createQueue":
		return true
	}
	return false
}

func writeMessage(conn net.Conn, messageType string, message string) {
	sendToClient(conn, `{"responseType": "`+messageType+
		`", "responseText": "`+message+`"}`)
}

func sendToClient(conn net.Conn, message string) {
	conn.Write([]byte(message + "\n"))
	log.Println(message)
	time.Sleep(1 * time.Millisecond)
}
