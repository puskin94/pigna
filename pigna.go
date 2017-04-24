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

	"github.com/satori/go.uuid"
)

type PignaConnection struct {
	Connection    net.Conn
	SenderName    string `json:"senderName"`
	IsConsuming   bool
	ConsumingList map[string]bool
}

type Response struct {
	ResponseType       string `json:"responseType"`
	ResponseTextString string `json:"responseTextString"`
	ResponseTextInt    int    `json:"responseTextInt"`
	ResponseTextBool   bool   `json:"ResponseTextBool"`
	SenderName         string `json:"senderName"`
	QueueName          string `json:"queueName"`
	MsgId              int    `json:"msgId"`
	MsgUUID            string `json:"UUID"`
	NeedsAck           bool   `json:"needsAck"`
	IsAChunk           bool   `json:"isAChunk"`
	NChunk             int    `json:"nChunk"`
	TotalChunks        int    `json:"totalChunks"`
}

// here will be stored the message chunks waiting to be complete
// the key is the MsgUUID
var chunked map[string](map[int]Response)

func Connect(host string, port string, filename string) (PignaConnection, error) {
	var pignaConn PignaConnection
	chunked = make(map[string](map[int]Response))
	pignaConn.ConsumingList = make(map[string]bool)

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
	res, _ := waitForResponse(*pignaConn)
	// stop the consuming go routine
	if res.ResponseType == "success" {
		pignaConn.ConsumingList[queueName] = true
	}
}

func (pignaConn *PignaConnection) RemoveConsumer(queueName string) (Response, error) {
	removeConsumer := `{"senderName": "` + pignaConn.SenderName +
		`", "action":"removeConsumer","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, removeConsumer)
	res, err := waitForResponse(*pignaConn)
	// stop the consuming go routine
	pignaConn.IsConsuming = false
	if res.ResponseType == "success" && pignaConn.ConsumingList[queueName] {
		pignaConn.ConsumingList[queueName] = false
	}
	return res, err
}

func (pignaConn PignaConnection) HasBeenAcked(queueName string, messageUUID uuid.UUID) (bool, error) {
	hasBeenAcked := fmt.Sprintf(`{"senderName": "%s", "action":"hasBeenAcked",`+
		`"queue":{"queueName":"%s"}, "message": {"UUID": "%s"}}`,
		pignaConn.SenderName, queueName, messageUUID)
	writeToClient(pignaConn.Connection, hasBeenAcked)
	res, err := waitForResponse(pignaConn)
	return res.ResponseTextBool, err
}

func (pignaConn PignaConnection) SendMsg(queueName string, message string) (uuid.UUID) {
	maxMessageLen := 512
	encodedMessage := base64.StdEncoding.EncodeToString([]byte(message))
	var messageChunks = make([]string, len(encodedMessage)/maxMessageLen+1)
	// Creating UUID Version 4. Only one even if the message is chunked.
	// different chunks will have the same UUID
	u1 := uuid.NewV4()
	// split the message in little chunks if it is grater than `maxMessageLen`
	if len(encodedMessage) > maxMessageLen {
		for i := 0; i < len(encodedMessage); i += maxMessageLen {
			if i+maxMessageLen <= len(encodedMessage) {
				log.Println(encodedMessage[i : i+maxMessageLen])
				messageChunks[int(i/maxMessageLen)] = encodedMessage[i : i+maxMessageLen]
			} else {
				messageChunks[int(i/maxMessageLen)] = encodedMessage[i:]
			}
		}
	} else {
		// send as an unique message, no need to add the property to the string
		sendMsg := fmt.Sprintf(`{"senderName": "%s", "action":"sendMsg",`+
			`"queue":{"queueName":"%s"}, "message": {"body": "%s", "UUID": "%s"}}`,
			pignaConn.SenderName, queueName, encodedMessage, u1)
		writeToClient(pignaConn.Connection, sendMsg)
		return u1
	}

	for i := 0; i < len(messageChunks); i++ {
		sendMsg := fmt.Sprintf(`{"senderName": "%s", "action":"sendMsg",`+
			`"queue":{"queueName":"%s"}, "message": {"body": "%s", `+
			`"isAChunk": %v, "nChunk": %d, "totalChunks": %d, "UUID": "%s"}}`,
			pignaConn.SenderName, queueName,
			messageChunks[i], true, i, len(messageChunks), u1)
		writeToClient(pignaConn.Connection, sendMsg)
	}
	return u1
}

func consume(pignaConn PignaConnection, callback func(PignaConnection, Response)) {
	chunkSize := 1024
	broken := ""
	for pignaConn.IsConsuming {
		var response Response

		var buffer = make([]byte, chunkSize)
		readLen, err := pignaConn.Connection.Read(buffer)

		if err != nil {
			log.Println("Connection closed by the server. Shutting down")
			break
		}
		buffer = buffer[:readLen]
		msgs := strings.Split(string(buffer[:readLen]), "\n")

		for msgIdx := 0; msgIdx < len(msgs); msgIdx++ {
			if len(msgs[msgIdx]) == 0 {
				continue
			}
			err := json.Unmarshal([]byte(msgs[msgIdx]), &response)
			if err != nil && len(msgs[msgIdx]) > 0 {
				if msgs[msgIdx][0] == '{' {
					broken = msgs[msgIdx]
					continue
				} else {
					_ = json.Unmarshal([]byte(broken+msgs[msgIdx]), &response)
					broken = ""
				}
			}

			// check if the received message is a part of a bigger one
			// if so, wait all the parts and then return it to the callback
			if response.IsAChunk {
				if _, isPresent := chunked[response.MsgUUID]; !isPresent {
					chunked[response.MsgUUID] = make(map[int]Response, response.TotalChunks)
				}
				chunked[response.MsgUUID][response.NChunk] = response
				// have I collected all the chunks?
				if len(chunked[response.MsgUUID]) != response.TotalChunks {
					continue
				}
				// reuse the last message
				response.ResponseTextString = ""
				for msgIdx := 0; msgIdx < response.TotalChunks; msgIdx++ {
					// collect all the messages and create a single
					response.ResponseTextString += chunked[response.MsgUUID][msgIdx].ResponseTextString
				}
				delete(chunked, response.MsgUUID)
			}

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
