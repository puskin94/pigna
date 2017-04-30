package pigna

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
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

type Request struct {
	SenderName string  `json:"senderName,omitempty"`
	Action     string  `json:"action"`
	Queue      Queue   `json:"queue,omitempty"`
	Message    Message `json:"message,omitempty"`
}

type Queue struct {
	QueueName string `json:"queueName,omitempty"`
	QueueType string `json:"queueType"`
	NeedsAck  bool   `json:"needsAck,omitempty"`
}

type Message struct {
	Body        string `json:"body,omitempty"`
	UUID        string `json:"UUID,omitempty"`
	IsAChunk    bool   `json:"isAChunk,omitempty"`
	NChunk      int    `json:"nChunk,omitempty"`
	TotalChunks int    `json:"totalChunks,omitempty"`
	MsgId       int    `json:"msgId,omitempty"`
}

// here will be stored the message chunks waiting to be complete
// the key is the MsgUUID
var chunked map[string](map[int]Response)

func (req *Request) String() string {
	reqString, _ := json.Marshal(req)
	return string(reqString)
}

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
	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "checkQueueName",
		Queue: Queue{
			QueueName: queueName,
		},
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)
	return res.ResponseTextBool, err
}

func (pignaConn PignaConnection) GetNumberOfPaired(queueName string) (int, error) {
	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "getNumOfPaired",
		Queue: Queue{
			QueueName: queueName,
		},
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)
	return res.ResponseTextInt, err
}

func (pignaConn PignaConnection) GetNumberOfUnacked(queueName string) (int, error) {
	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "getNumOfUnacked",
		Queue: Queue{
			QueueName: queueName,
		},
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)
	return res.ResponseTextInt, err
}

func (pignaConn PignaConnection) GetNumberOfUnconsumed(queueName string) (int, error) {
	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "getNumOfUnconsumed",
		Queue: Queue{
			QueueName: queueName,
		},
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)
	return res.ResponseTextInt, err
}

func (pignaConn PignaConnection) GetNamesOfPaired(queueName string) ([]string, error) {
	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "getNamesOfPaired",
		Queue: Queue{
			QueueName: queueName,
		},
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)
	stringSlice := strings.Split(res.ResponseTextString, ",")

	return stringSlice, err
}

func (pignaConn PignaConnection) GetQueueNames() ([]string, error) {
	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "getQueueNames",
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)
	stringSlice := strings.Split(res.ResponseTextString, ",")

	return stringSlice, err
}

func (pignaConn PignaConnection) CreateQueue(queueName string, queueType string, needsAck bool) (Response, error) {
	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "createQueue",
		Queue: Queue{
			QueueName: queueName,
			QueueType: queueType,
			NeedsAck:  needsAck,
		},
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)
	return res, err
}

func (pignaConn PignaConnection) DestroyQueue(queueName string) (Response, error) {
	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "destroyQueue",
		Queue: Queue{
			QueueName: queueName,
		},
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)
	return res, err
}

func (pignaConn *PignaConnection) ConsumeQueue(queueName string, callback func(PignaConnection, Response)) {
	pignaConn.IsConsuming = true
	go consume(*pignaConn, callback)

	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "consumeQueue",
		Queue: Queue{
			QueueName: queueName,
		},
	}
	writeToClient(pignaConn.Connection, req.String())
	res, _ := waitForResponse(*pignaConn)
	// stop the consuming go routine
	if res.ResponseType == "success" {
		pignaConn.ConsumingList[queueName] = true
	}
}

func (pignaConn *PignaConnection) RemoveConsumer(queueName string) (Response, error) {
	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "removeConsumer",
		Queue: Queue{
			QueueName: queueName,
		},
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(*pignaConn)
	// stop the consuming go routine
	pignaConn.IsConsuming = false
	if res.ResponseType == "success" && pignaConn.ConsumingList[queueName] {
		pignaConn.ConsumingList[queueName] = false
	}
	return res, err
}

func (pignaConn PignaConnection) HasBeenAcked(queueName string, messageUUID uuid.UUID) (bool, error) {
	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "hasBeenAcked",
		Queue: Queue{
			QueueName: queueName,
		},
		Message: Message{
			UUID: messageUUID.String(),
		},
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)
	return res.ResponseTextBool, err
}

func (pignaConn PignaConnection) SendMsg(queueName string, message string) uuid.UUID {
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
				messageChunks[int(i/maxMessageLen)] = encodedMessage[i : i+maxMessageLen]
			} else {
				messageChunks[int(i/maxMessageLen)] = encodedMessage[i:]
			}
		}
	} else {
		// send as an unique message, no need to add the property to the string
		var req Request = Request{
			SenderName: pignaConn.SenderName,
			Action:     "sendMsg",
			Queue: Queue{
				QueueName: queueName,
			},
			Message: Message{
				Body: encodedMessage,
				UUID: u1.String(),
			},
		}
		writeToClient(pignaConn.Connection, req.String())
		return u1
	}

	var req Request = Request{
		SenderName: pignaConn.SenderName,
		Action:     "sendMsg",
		Queue: Queue{
			QueueName: queueName,
		},
	}

	for i := 0; i < len(messageChunks); i++ {
		req.Message = Message{
			Body:        messageChunks[i],
			IsAChunk:    true,
			NChunk:      i,
			TotalChunks: len(messageChunks),
			UUID:        u1.String(),
		}
		writeToClient(pignaConn.Connection, req.String())
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
					var req Request = Request{
						SenderName: pignaConn.SenderName,
						Action:     "msgAck",
						Queue: Queue{
							QueueName: response.QueueName,
						},
						Message: Message{
							MsgId: response.MsgId,
						},
					}
					writeToClient(pignaConn.Connection, req.String())
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
