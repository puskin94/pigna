package pignaDaemon

import (
	"bufio"
	"errors"
	"log"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/puskin94/pigna"
	"gopkg.in/vmihailenco/msgpack.v2"
)

/*
~21s 1000000 with ack
~14s 1000000 without ack
*/

type MsgAction struct {
	Action     string
	SenderName string  `msgpack:",omitempty"`
	Message    Message `msgpack:",omitempty"`
	Queue      Queue   `msgpack:",omitempty"`
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
	QueueName          string
	QueueType          string
	NeedsAck           bool
	HostOwner          string
	ClientConn         net.Conn
	ServerQueue        Client
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
	Body        string `msgpack:",omitempty"`
	MsgId       int    `msgpack:",omitempty"`
	MsgUUID     string `msgpack:",omitempty"`
	IsAChunk    bool   `msgpack:",omitempty"`
	NChunk      int    `msgpack:",omitempty"`
	TotalChunks int    `msgpack:",omitempty"`
	SenderName  string `msgpack:",omitempty"`
	SenderConn  net.Conn
}

var validActions = map[string]func(net.Conn, MsgAction){
	"getNumOfPaired":     actionGetNumberOfPaired,
	"createQueue":        actionCreateQueue,
	"checkQueueName":     actionCheckQueueName,
	"getNamesOfPaired":   actionGetNamesOfPaired,
	"getQueueNames":      actionGetQueueNames,
	"getNumOfUnacked":    actionGetNumOfUnacked,
	"getNumOfUnconsumed": actionGetNumOfUnconsumed,
	"getNumOfQueues":     actionGetNumOfQueues,
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

func (q *Queue) addConsumer(forwardConn net.Conn, senderName string) []Client {
	var c Client
	c.ForwardConn = forwardConn
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

	sendToDaemon(mainPigna, req)
	return err
}

func handleRequest(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := scanner.Text()
		// log.Println(msg)

		msgAct := new(MsgAction)
		err := msgpack.Unmarshal([]byte(msg), &msgAct)
		if err != nil {
			writeMessageString(conn, "error", "Invalid msgpack request. "+err.Error())
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

func broadcastToQueue(q *Queue, message Message) {
	// send the body to all the Consumers connections
	for idx, _ := range q.Consumers {
		msg := formatMessage(*q, message)
		if q.NeedsAck {
			q.UnackedMessages = append(q.UnackedMessages, message)
		}
		// log.Println(q.UnackedMessages)
		sendToClient(q.Consumers[idx].ForwardConn, msg)
	}
}

func formatMessage(q Queue, message Message) pigna.Response {
	var msg pigna.Response = pigna.Response{
		ResponseType:       "recvMsg",
		QueueName:          q.QueueName,
		ResponseTextString: message.Body,
		SenderName:         message.SenderName,
		MsgId:              message.MsgId,
		NeedsAck:           q.NeedsAck,
		IsAChunk:           message.IsAChunk,
		NChunk:             message.NChunk,
		TotalChunks:        message.TotalChunks,
		MsgUUID:            message.MsgUUID,
	}
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

func getPort(addr net.Addr) string {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", addr.String())
	return strconv.Itoa(tcpAddr.Port)
}

func writeMessageString(conn net.Conn, messageType string, message string) {
	var msg pigna.Response = pigna.Response{
		ResponseType:       messageType,
		ResponseTextString: message,
	}
	sendToClient(conn, msg)
}

func writeMessageQueue(conn net.Conn, messageType string, queue pigna.Queue) {
	var msg pigna.Response = pigna.Response{
		ResponseType: messageType,
		QueueInfo:    queue,
	}
	sendToClient(conn, msg)
}

func writeMessageInt(conn net.Conn, messageType string, message int) {
	var msg pigna.Response = pigna.Response{
		ResponseType:    messageType,
		ResponseTextInt: message,
	}
	sendToClient(conn, msg)
}

func writeMessageBool(conn net.Conn, messageType string, message bool) {
	var msg pigna.Response = pigna.Response{
		ResponseType:     messageType,
		ResponseTextBool: message,
	}
	sendToClient(conn, msg)
}

func sendToClient(conn net.Conn, message pigna.Response) {
	encoded, _ := msgpack.Marshal(message)
	conn.Write(append(encoded, "\n"...))
	// log.Println(message)
}

func sendToDaemon(conn net.Conn, message MsgAction) {
	encoded, _ := msgpack.Marshal(message)
	conn.Write(append(encoded, "\n"...))
}
