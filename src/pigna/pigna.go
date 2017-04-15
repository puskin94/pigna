package pigna

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strings"
	"time"
)

type PignaConnection struct {
	Connection  net.Conn
	SenderName  string `json:"senderName"`
	IsConsuming bool
}

type Response struct {
	ResponseType       string `json:"responseType"`
	ResponseTextString string `json:"responseTextString"`
	ResponseTextInt    int    `json:"responseTextInt"`
	ResponseTextBool   bool   `json:"ResponseTextBool"`
	SenderName         string `json:"senderName"`
	QueueName          string `json:"queueName"`
	MsgId              int    `json:"msgId"`
	NeedsAck           bool   `json:"needsAck"`
}

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
	checkQueueName := fmt.Sprintf(`{"senderName": "%s", `+
		`"action":"checkQueueName","queue":{"queueName":"%s"}}`,
		pignaConn.SenderName, queueName)
	writeToClient(pignaConn.Connection, checkQueueName)
	res, err := waitForResponse(pignaConn)
	return res.ResponseTextBool, err
}

func (pignaConn PignaConnection) GetNumberOfPaired(queueName string) (int, error) {
	getNumber := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"getNumOfPaired","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, getNumber)
	res, err := waitForResponse(pignaConn)
	return res.ResponseTextInt, err
}

func (pignaConn PignaConnection) GetNumberOfUnacked(queueName string) (int, error) {
	getNumber := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"getNumOfUnacked","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, getNumber)
	res, err := waitForResponse(pignaConn)
	return res.ResponseTextInt, err
}

func (pignaConn PignaConnection) GetNumberOfUnconsumed(queueName string) (int, error) {
	getNumber := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"getNumOfUnconsumed","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, getNumber)
	res, err := waitForResponse(pignaConn)
	return res.ResponseTextInt, err
}

func (pignaConn PignaConnection) GetNamesOfPaired(queueName string) ([]string, error) {
	getNames := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"getNamesOfPaired","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, getNames)
	res, err := waitForResponse(pignaConn)
	stringSlice := strings.Split(res.ResponseTextString, ",")

	return stringSlice, err
}

func (pignaConn PignaConnection) GetQueueNames() ([]string, error) {
	getNames := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"getQueueNames","queue":{}}`
	writeToClient(pignaConn.Connection, getNames)
	res, err := waitForResponse(pignaConn)
	stringSlice := strings.Split(res.ResponseTextString, ",")

	return stringSlice, err
}

func (pignaConn PignaConnection) CreateQueue(queueName string, needsAck bool) (Response, error) {
	createQueue := fmt.Sprintf(`{"senderName": "%s", "action":"createQueue",`+
		`"queue":{"queueName":"%s", "needsAck": %t}}`,
		pignaConn.SenderName, queueName, needsAck)
	writeToClient(pignaConn.Connection, createQueue)
	res, err := waitForResponse(pignaConn)
	// if this queue needs an ack, add it to the proper map

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

func (pignaConn *PignaConnection) ConsumeQueue(queueName string, callback func(PignaConnection, Response)) {
	pignaConn.IsConsuming = true
	go consume(*pignaConn, callback)
	consumeQueue := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"consumeQueue","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, consumeQueue)

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
	sendMsg := fmt.Sprintf(`{"senderName": "%s", "action":"sendMsg",`+
		`"queue":{"queueName":"%s"}, "message": {"body": "%s"}}`,
		pignaConn.SenderName, queueName,
		base64.StdEncoding.EncodeToString([]byte(message)))
	writeToClient(pignaConn.Connection, sendMsg)
}

func consume(pignaConn PignaConnection, callback func(PignaConnection, Response)) {
	for pignaConn.IsConsuming {
		var response Response
		message, err := bufio.NewReader(pignaConn.Connection).ReadString('\n')
		if err != nil {
			log.Println("Connection closed by the server. Shutting down")
			break
		}
		_ = json.Unmarshal([]byte(message), &response)
		dec, _ := base64.StdEncoding.DecodeString(response.ResponseTextString)
		response.ResponseTextString = string(dec[:])
		if response.ResponseType == "recvMsg" {
			if response.NeedsAck {
				msgAck := fmt.Sprintf(`{"senderName": "%s", "action":"msgAck"`+
					`,"queue":{"queueName":"%s"}, "message": {"msgId": %d}}`,
					pignaConn.SenderName, response.QueueName, response.MsgId)
				writeToClient(pignaConn.Connection, msgAck)
			}
			callback(pignaConn, response)
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
