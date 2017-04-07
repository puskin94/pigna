package pigna

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"log"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type PignaConnection struct {
	Connection  net.Conn
	SenderName  string `json:"senderName"`
	IsConsuming bool

}

type Response struct {
	ResponseType string `json:"responseType"`
	ResponseText string `json:"responseText"`
	SenderName   string `json:"senderName"`
	QueueName    string `json:"queueName"`
	MsgId        string `json:"msgId"`
}

var QueuesAck map[string]bool // this contains queueNames that are requesting acks

func Connect(host string, port string, filename string) (PignaConnection, error) {
	var pignaConn PignaConnection

	// read the config file
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		return pignaConn, err
	}

	json.Unmarshal(raw, &pignaConn)

	conn, err := net.Dial("tcp", host+":"+port)
	pignaConn.Connection = conn
	pignaConn.IsConsuming = false
	return pignaConn, err
}

func (pignaConn PignaConnection) Disconnect() {
	log.Println("Pigna will quit when stop consuming...")
	for pignaConn.IsConsuming {
		time.Sleep(1000 * time.Millisecond)
	}
	pignaConn.Connection.Close()
}

func (pignaConn PignaConnection) CheckQueueName(queueName string) (bool, error) {
	checkQueueName := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"checkQueueName","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, checkQueueName)
	res, err := waitForResponse(pignaConn)
	boo, _ := strconv.ParseBool(res.ResponseText)
	return boo, err
}

func (pignaConn PignaConnection) GetNumberOfPaired(queueName string) (int, error) {
	getNumber := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"getNumberOfPaired","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, getNumber)
	res, err := waitForResponse(pignaConn)
	num, _ := strconv.Atoi(res.ResponseText)
	return num, err
}

func (pignaConn PignaConnection) GetNamesOfPaired(queueName string) ([]string, error) {
	getNames := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"getNamesOfPaired","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, getNames)
	res, err := waitForResponse(pignaConn)
	stringSlice := strings.Split(res.ResponseText, ",")

	return stringSlice, err
}

func (pignaConn PignaConnection) CreateQueue(queueName string, needsAck bool) (Response, error) {
	createQueue := fmt.Sprintf(`{"senderName": "%s", "action":"createQueue",`+
		`"queue":{"queueName":"%s", "needsAck": %t}}`,
		pignaConn.SenderName, queueName, needsAck)
	writeToClient(pignaConn.Connection, createQueue)
	res, err := waitForResponse(pignaConn)
	// if this queue needs an ack, add it to the proper map
	if err == nil {
		QueuesAck[queueName] = true
	}
	return res, err
}

func (pignaConn PignaConnection) DestroyQueue(queueName string) (Response, error) {
	destroyQueue := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"destroyQueue","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, destroyQueue)
	res, err := waitForResponse(pignaConn)
	return res, err
}

func (pignaConn *PignaConnection) ConsumeQueue(queueName string, callback func(Response)) {
	consumeQueue := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"consumeQueue","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, consumeQueue)

	pignaConn.IsConsuming = true
	go consume(*pignaConn, callback)
}

func (pignaConn *PignaConnection) RemoveConsumer(queueName string) (Response, error) {
	removeConsumer := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"removeConsumer","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, removeConsumer)
	res, err := waitForResponse(*pignaConn)
	// stop the consuming go routine
	pignaConn.IsConsuming = false
	return res, err
}

func (pignaConn PignaConnection) SendMsg(queueName string, message string) {
	sendMsg := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"sendMsg","queue":{"queueName":"` +
		queueName + `"}, "message": {"body": "` + message + `"}}`
	writeToClient(pignaConn.Connection, sendMsg)
}

func consume(pignaConn PignaConnection, callback func(Response)) {
	for pignaConn.IsConsuming {
		var response Response
		message, err := bufio.NewReader(pignaConn.Connection).ReadString('\n')
		if err != nil {
			log.Println("Connection closed by the server. Shutting down")
			break
		}
		_ = json.Unmarshal([]byte(message), &response)
		if response.ResponseType == "recvMsg" {
			if QueuesAck[response.QueueName] {
				msgAck := fmt.Sprintf(`{"senderName": "%s", "action":"msgAck"` +
					`,"queue":{"queueName":"%s"}, "message": {"msgId": %d}}`,
					pignaConn.SenderName, response.QueueName, response.MsgId)
				writeToClient(pignaConn.Connection, msgAck)
			}
			callback(response)
		}
	}
}

func waitForResponse(pignaConn PignaConnection) (Response, error) {
	var response Response
	message, _ := bufio.NewReader(pignaConn.Connection).ReadString('\n')
	err := json.Unmarshal([]byte(message), &response)

	return response, err
}

func writeToClient(conn net.Conn, message string) {
	conn.Write([]byte(message + "\n"))
}
