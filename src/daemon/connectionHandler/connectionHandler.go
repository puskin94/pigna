package connectionHandler

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
	"sort"
	"sync"
	"time"
)

type MsgAction struct {
	Action  string  `json:"action"`
	Message Message `json:"message"`
	Queue   Queue   `json:"queue"`
}

type QueueList struct {
	Queues []Queue
}

type Queue struct {
	QueueName          string `json:"queueName"`
	Consumers          []net.Conn
	UnconsumedMessages []Message
	MutexCounter       sync.Mutex
	MsgCounter         int
}

type Message struct {
	Body      string `json:"body"`
	Timestamp string `json:"timestamp"`
	Sender    net.Conn
	MsgId     int
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

func (q *QueueList) AddQueue(newQueue Queue) []Queue {
	q.Queues = append(q.Queues, newQueue)
	return q.Queues
}

func (q *QueueList) DestroyQueue(queueIdx int) []Queue {
	q.Queues[queueIdx] = q.Queues[len(q.Queues)-1]
	q.Queues = q.Queues[:len(q.Queues)-1]
	return q.Queues
}

func (q *Queue) AddConsumer(conn net.Conn) []net.Conn {
	q.Consumers = append(q.Consumers, conn)
	return q.Consumers
}

func (q *Queue) AddUnconsumedMessage(message Message) []Message {
	q.UnconsumedMessages = append(q.UnconsumedMessages, message)
	return q.UnconsumedMessages
}

func (q *Queue) DeleteConsumer(connIdx int) []net.Conn {
	q.Consumers[connIdx] = q.Consumers[len(q.Consumers)-1]
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
			writeMessage(conn, "error", "Invalid JSON request")
			return
		}

		errAction, resType, resText := checkMsgAction(msgAct)
		if errAction {
			writeMessage(conn, resType, resText)
			return
		}

		if msgAct.Action == "createQueue" {
			if exists, _ := checkQueueName(msgAct.Queue); exists == true {
				writeMessage(conn, "error", "This queueName already exists")
				return
			}
			var newQueue Queue
			newQueue.MsgCounter = 0
			copyQueueStruct(msgAct, &newQueue)
			queueList.AddQueue(newQueue)
			writeMessage(conn, "success", "Queue "+msgAct.Queue.QueueName+
				" created successfully")

		} else if msgAct.Action == "consumeQueue" {
			exists, queueIdx := checkQueueName(msgAct.Queue)
			if !exists {
				writeMessage(conn, "error", "This queueName does not exists")
				return
			}
			alreadyConsuming, _ := checkConsumers(conn, queueIdx)
			if alreadyConsuming {
				writeMessage(conn, "error", "Already consuming this queue")
				return
			}

			// adding the connection to the proper queue `Consumers`
			queueList.Queues[queueIdx].AddConsumer(conn)

			unconsumed := queueList.Queues[queueIdx].UnconsumedMessages

			// clear the queue sending the `UnconsumedMessages`
			go func() {
				sort.Sort(MessageSorter(unconsumed))
				for msgIdx, _ := range unconsumed {
					// do not send if the sender is the only one `Consumers`
					if unconsumed[msgIdx].Sender == conn &&
						len(queueList.Queues[queueIdx].Consumers) == 1 {
						continue
					}

					broadcastToQueue(queueList.Queues[queueIdx], unconsumed[msgIdx], conn)
					queueList.Queues[queueIdx].UnconsumedMessages = queueList.Queues[queueIdx].UnconsumedMessages[1:]
				}
			}()

		} else if msgAct.Action == "sendMsg" {
			exists, queueIdx := checkQueueName(msgAct.Queue)
			if !exists {
				writeMessage(conn, "error", "This queueName does not exists")
				return
			}

			msgAct.Message.Sender = conn
			queueList.Queues[queueIdx].MutexCounter.Lock()
			queueList.Queues[queueIdx].MsgCounter++
			msgAct.Message.MsgId = queueList.Queues[queueIdx].MsgCounter
			queueList.Queues[queueIdx].MutexCounter.Unlock()

			if len(queueList.Queues[queueIdx].Consumers) <= 1 {
				queueList.Queues[queueIdx].AddUnconsumedMessage(msgAct.Message)
			} else {
				go broadcastToQueue(queueList.Queues[queueIdx],
					msgAct.Message, conn)
			}
		} else if msgAct.Action == "destroyQueue" {
			exists, queueIdx := checkQueueName(msgAct.Queue)
			if !exists {
				writeMessage(conn, "error", "This queueName does not exists")
				return
			}
			queueList.DestroyQueue(queueIdx)

			writeMessage(conn, "success", "Queue "+msgAct.Queue.QueueName+
				" destroyed")

		} else if msgAct.Action == "removeConsumer" {
			exists, queueIdx := checkQueueName(msgAct.Queue)
			if !exists {
				writeMessage(conn, "error", "This queueName does not exists")
				return
			}
			alreadyConsuming, connIdx := checkConsumers(conn, queueIdx)
			if !alreadyConsuming {
				writeMessage(conn, "error", "You are not consuming this Queue")
				return
			}

			queueList.Queues[queueIdx].DeleteConsumer(connIdx)

			writeMessage(conn, "success", "You are not consuming anymore")
		}
	}
	if errRead := scanner.Err(); errRead != nil {
		log.Println("Client disconnected...")
		return
	}
}

func broadcastToQueue(q Queue, message Message, sender net.Conn) {
	// send the body to all the Consumers connections
	for idx, _ := range q.Consumers {
		if message.Sender == sender {
			continue
		}
		msg := `{"responseType":"recvMsg", "queueName":"` + q.QueueName + `","responseText": "` + message.Body + `"}`
		sendToClient(q.Consumers[idx], msg)
	}
}

func checkConsumers(conn net.Conn, queueIdx int) (bool, int) {
	for idx, _ := range queueList.Queues[queueIdx].Consumers {
		if conn == queueList.Queues[queueIdx].Consumers[idx] {
			return true, idx
		}
	}
	return false, -1
}

func checkQueueName(queue Queue) (bool, int) {
	for idx, _ := range queueList.Queues {
		if queue.QueueName == queueList.Queues[idx].QueueName {
			return true, idx
		}
	}
	return false, -1
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
