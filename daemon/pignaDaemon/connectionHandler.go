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
	SenderName string  `json:"senderName,omitempty"`
	Message    Message `json:"message,omitempty"`
	Queue      Queue   `json:"queue,omitempty"`
}

type ClusterNode struct {
	QueueList  *QueueList
	Connection net.Conn
	Port       string
}

type QueueList struct {
	Queues map[string]*Queue
}

type Queue struct {
	QueueName          string   `json:"queueName"`
	QueueType          string   `json:"queueType"`
	NeedsAck           bool     `json:"needsAck"`
	HostOwner          string   `json:"hostOwner,omitempty"`
	ClientConn         net.Conn `json:"clientConn,omitempty"`
	Consumers          []Client
	UnconsumedMessages []Message
	UnackedMessages    []Message
	MutexCounter       sync.Mutex
	MsgCounter         int
	LastRRIdx          int
}

type Client struct {
	ForwardConn net.Conn
	ForwardPort string
	Name        string
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

var validActions = map[string]func(net.Conn, MsgAction){
	"getNumOfPaired":     actionGetNumberOfPaired,
	"createQueue":        actionCreateQueue,
	"checkQueueName":     actionCheckQueueName,
	"consumeQueue":       actionConsumeQueue,
	"getNamesOfPaired":   actionGetNamesOfPaired,
	"getQueueNames":      actionGetQueueNames,
	"getNumOfUnacked":    actionGetNumOfUnacked,
	"getNumOfUnconsumed": actionGetNumOfUnconsumed,
	"getNumOfQueues":     actionGetNumOfQueues,
	"sendMsg":            actionSendMsg,
	"msgAck":             actionAckMessage,
	"hasBeenAcked":       actionHasBeenAcked,
	"destroyQueue":       actionDestroyQueue,
	"removeConsumer":     actionRemoveConsumer,
	"newClusterNode":     actionAddClusterNode,
}

var thisPort string
var thisHost string
var thisIsANode bool
var clusterHost string
var clusterPort string
var queueList QueueList
var clusterNodes map[string]ClusterNode
var waitingForCreateResponse map[string]net.Conn

func (q *QueueList) addQueue(newQueue Queue) map[string]*Queue {
	q.Queues[newQueue.QueueName] = &newQueue
	return q.Queues
}

func (q *QueueList) destroyQueue(queueName string) map[string]*Queue {
	delete(q.Queues, queueName)
	return q.Queues
}

func (q *Queue) addConsumer(forwardConn net.Conn, forwardPort string, senderName string) []Client {
	var c Client
	c.ForwardConn = forwardConn
	c.ForwardPort = forwardPort
	c.Name = senderName
	q.Consumers = append(q.Consumers, c)
	return q.Consumers
}

func (q *Queue) addUnconsumedMessage(message Message) []Message {
	q.UnconsumedMessages = append(q.UnconsumedMessages, message)
	return q.UnconsumedMessages
}

func StartServer(host, port, ch, cp string) {
	l, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	thisPort = port
	thisHost, ipErr := getLocalIp()
	if ipErr != nil {
		log.Println("Error getting local ip")
		return
	}

	// this pignaDaemon will be a clustered instance of a main pignaDaemon
	if ch != "" && thisHost != "" && cp != "" {
		clusterHost = ch
		clusterPort = cp
		errMainPigna := askToJoinAsNodeCluster(ch, cp)
		if errMainPigna != nil {
			log.Println("Error connecting to :", clusterHost, errMainPigna.Error())
			return
		}
		thisIsANode = true
	}

	queueList.Queues = make(map[string]*Queue)
	clusterNodes = make(map[string]ClusterNode)
	waitingForCreateResponse = make(map[string]net.Conn)

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

func askToJoinAsNodeCluster(clusterHost, clusterPort string) error {
	mainPigna, err := net.Dial("tcp", clusterHost+":"+clusterPort)

	// act like a normal pigna request
	req := MsgAction{
		Action: "newClusterNode",
		Message: Message{
			Body: thisHost,
		},
	}

	reqString, _ := json.Marshal(req)
	sendToClient(mainPigna, string(reqString))
	return err
}

func handleRequest(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text()
		// log.Println(msg)

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

func (q *Queue) deleteConsumer(clIdx int) []Client {
	q.Consumers[clIdx] = q.Consumers[len(q.Consumers)-1]
	q.Consumers = q.Consumers[:len(q.Consumers)-1]
	return q.Consumers
}

func broadcastToQueue(q Queue, message Message) {
	// send the body to all the Consumers connections
	for idx, _ := range q.Consumers {
		msg := formatMessage(q, message)
		if q.NeedsAck {
			q.UnackedMessages = append(q.UnackedMessages, message)
		}
		sendToClient(q.Consumers[idx].ForwardConn, msg)
	}
}

func formatMessage(q Queue, message Message) string {
	msg := fmt.Sprintf(`{"responseType":"recvMsg", "queueName":"%s", `+
		`"responseTextString": "%s", "senderName": "%s", "msgId": %d,`+
		`"needsAck": %v, "isAChunk": %v, "nChunk": %d, "totalChunks": %d}`,
		q.QueueName, message.Body, message.SenderName,
		message.MsgId, q.NeedsAck, message.IsAChunk, message.NChunk,
		message.TotalChunks)
	return msg
}

func checkConsumers(conn net.Conn, queueName string, consumerName string) (int, error) {
	for idx, _ := range queueList.Queues[queueName].Consumers {
		if conn == queueList.Queues[queueName].Consumers[idx].ForwardConn ||
			consumerName == queueList.Queues[queueName].Consumers[idx].Name {
			return idx, nil
		}
	}
	return -1, errors.New("No consumer on this queue")
}

func checkQueueName(queue Queue) (bool, string, error) {
	// search locally
	_, isPresentLocally := queueList.Queues[queue.QueueName]
	if isPresentLocally {
		return true, "", nil
	}

	// search inside cluster nodes
	for hostname, node := range clusterNodes {
		_, isPresentOnCluster := node.QueueList.Queues[queue.QueueName]
		if isPresentOnCluster {
			return true, hostname, nil
		}
	}
	return false, "", errors.New("No queue with this name")
}

func copyQueueStruct(m *MsgAction, q *Queue) {
	q.QueueName = m.Queue.QueueName
	q.NeedsAck = m.Queue.NeedsAck
	q.QueueType = m.Queue.QueueType
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
	if m.Queue.QueueName == "" &&
		m.Action != "getQueueNames" &&
		m.Action != "newClusterNode" &&
		m.Action != "getNumOfQueues" {

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

func getLocalIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
    if err != nil {
        return "", err
    }
    for _, address := range addrs {
        // check the address type and if it is not a loopback then display it
        if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
            if ipnet.IP.To4() != nil {
                return ipnet.IP.String(), nil
            }
        }
    }
    return "", err
}

func getHostname(conn net.Conn) string {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", conn.RemoteAddr().String())
	return tcpAddr.IP.String()
}

func writeMessageString(conn net.Conn, messageType string, message string) {
	msg := fmt.Sprintf(`{"responseType": "%s", "responseTextString": "%s"}`,
		messageType, message)
	sendToClient(conn, msg)
}

func writeMessageStringEncoded(conn net.Conn, messageType string, message string) {
	msg := fmt.Sprintf(`{"responseType": "%s", "responseTextStringEncoded": "%s"}`,
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
	_, err := conn.Write([]byte(message + "\n"))
	// log.Println(message)
	if err != nil {
		log.Println(err)
	}
}
