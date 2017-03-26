package connectionHandler

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
)

type MsgAction struct {
	Action  string `json:"action"`
	Message Message `json:"message"`
	Queue   Queue  `json:"queue"`
}

type QueueList struct {
	Queues []Queue
}

type Queue struct {
	QueueName			string `json:"queueName"`
	Consumers			[]net.Conn
	UnconsumedMessages	[]Message
}

type Message struct {
	Body		string `json:"body"`
	Timestamp	string
	Sender		net.Conn
}

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

func (q *Queue) DestroyConsumedMessage(msgIdx int) []Message {
	q.UnconsumedMessages[msgIdx] = q.UnconsumedMessages[len(q.UnconsumedMessages)-1]
	q.UnconsumedMessages = q.UnconsumedMessages[:len(q.UnconsumedMessages)-1]
	return q.UnconsumedMessages
}

func (q *Queue) DeleteConsumer(connIdx int) []net.Conn {
	q.Consumers[connIdx] = q.Consumers[len(q.Consumers)-1]
	q.Consumers = q.Consumers[:len(q.Consumers)-1]
	return q.Consumers
}

func handleRequest(conn net.Conn) {
	for {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			msg := scanner.Text()
			log.Println("Client sends: " + msg)
			msgAct := new(MsgAction)
			err := json.Unmarshal([]byte(msg), &msgAct)
			if err != nil {
				writeToClient(conn, `{"responseType": "error", "responseText": "Invalid JSON request"}`)
				break
			}
			errMsgAction := checkMsgAction(msgAct)
			if errMsgAction != "" {
				writeToClient(conn, errMsgAction)
				break
			}

			if msgAct.Action == "createQueue" {
				if exists, _ := checkQueueName(msgAct.Queue); exists == true {
					writeToClient(conn, `{"responseType": "error", "responseText": "This queueName already exists"}`)
					break
				}
				var newQueue Queue
				copyQueueStruct(msgAct, &newQueue)
				queueList.AddQueue(newQueue)
				writeToClient(conn, `{"responseType": "success", "responseText": "Queue created successfully"}`)

			} else if msgAct.Action == "consumeQueue" {
				exists, queueIdx := checkQueueName(msgAct.Queue)
				if exists == false {
					writeToClient(conn, `{"responseType": "error", "responseText": "This queueName does not exists"}`)
					break
				}
				alreadyConsuming, _ := checkConsumers(conn, queueIdx)
				if alreadyConsuming == true {
					writeToClient(conn, `{"responseType": "error", "responseText": "Already consuming this queue"}`)
					break
				}

				// adding the connection to the proper queue `Consumers`
				queueList.Queues[queueIdx].AddConsumer(conn)

				// clear the queue sending the `UnconsumedMessages`
				unconsumed := queueList.Queues[queueIdx].UnconsumedMessages
				for msgIdx, _ := range unconsumed {
					// XXX: check timestamp!

					// do not send if the sender is the only one `Consumers`
					if (unconsumed[msgIdx].Sender == conn &&
						len(queueList.Queues[queueIdx].Consumers) == 1) {continue}

					go broadcastToQueue(queueList.Queues[queueIdx], unconsumed[msgIdx], conn)
					// XXX: delete in order by timestamp?
					queueList.Queues[queueIdx].DestroyConsumedMessage(msgIdx)
				}

			} else if msgAct.Action == "sendMsg" {
				exists, queueIdx := checkQueueName(msgAct.Queue)
				if exists == false {
					writeToClient(conn, `{"responseType": "error", "responseText": "This queueName does not exists"}`)
					break
				}

				msgAct.Message.Sender = conn

				if len(queueList.Queues[queueIdx].Consumers) <= 1 {
					queueList.Queues[queueIdx].AddUnconsumedMessage(msgAct.Message)
				}

				go broadcastToQueue(queueList.Queues[queueIdx], msgAct.Message, conn)

			} else if msgAct.Action == "destroyQueue" {
				exists, queueIdx := checkQueueName(msgAct.Queue)
				if exists == false {
					writeToClient(conn, `{"responseType": "error", "responseText": "This queueName does not exists"}`)
					break
				}
				queueList.DestroyQueue(queueIdx)

			} else if msgAct.Action == "removeConsumer" {
				exists, queueIdx := checkQueueName(msgAct.Queue)
				if exists == false {
					writeToClient(conn, `{"responseType": "error", "responseText": "This queueName does not exists"}`)
					break
				}
				alreadyConsuming, connIdx := checkConsumers(conn, queueIdx)
				if alreadyConsuming == false {
					writeToClient(conn, `{"responseType": "error", "responseText": "You are not consuming this Queue"}`)
					break
				}

				queueList.Queues[queueIdx].DeleteConsumer(connIdx)
			}
		}
		if errRead := scanner.Err(); errRead != nil {
			log.Println("Client disconnected...")
			break
		}
	}
}

func broadcastToQueue(q Queue, message Message, sender net.Conn) {
	// send the body to all the Consumers connections
	for idx, _ := range q.Consumers {
		if message.Sender == sender {continue}
		writeToClient(q.Consumers[idx], message.Body)
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

func checkMsgAction(m *MsgAction) string {
	var err string
	if isValidAction(m.Action) == false {err = `{"responseType": "error", "responseText": "Invalid Action"}`}
	if err == "" {err = checkMsgParameters(m)}
	return err
}

func checkMsgParameters(m *MsgAction) string {
	// `QueueName` is mandatory for every message type
	if m.Queue.QueueName == "" {return`{"responseType": "error", "responseText": "Invalid queueName"}`}

	if m.Action == "sendMsg" {
		if m.Message.Body == "" {
			return `{"responseType": "error", "responseText": "Missing the 'body' param"}`
		}
	}
	return ""
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

func writeToClient(conn net.Conn, message string) {
	conn.Write([]byte(message + "\n"))
}
