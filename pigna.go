package pigna

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/satori/go.uuid"
)

type PignaConnection struct {
	Connection net.Conn `json:"connection,omitempty"`
	Hostname   string   `json:"hostname,omitempty"`
	Port       string   `json:"port,omitempty"`
}

type Response struct {
	ResponseType              string `json:"responseType"`
	ResponseTextString        string `json:"responseTextString"`
	ResponseTextStringEncoded string `json:"responseTextStringEncoded"`
	ResponseTextInt           int    `json:"responseTextInt"`
	ResponseTextBool          bool   `json:"responseTextBool"`
	SenderName                string `json:"senderName"`
	QueueName                 string `json:"queueName"`
	MsgId                     int    `json:"msgId"`
	MsgUUID                   string `json:"UUID"`
	NeedsAck                  bool   `json:"needsAck"`
	IsAChunk                  bool   `json:"isAChunk"`
	NChunk                    int    `json:"nChunk"`
	TotalChunks               int    `json:"totalChunks"`
}

type Request struct {
	SenderName string  `json:"senderName,omitempty"`
	Action     string  `json:"action"`
	Queue      Queue   `json:"queue,omitempty"`
	Message    Message `json:"message,omitempty"`
}

type Queue struct {
	QueueName     string          `json:"queueName,omitempty"`
	QueueType     string          `json:"queueType,omitempty"`
	NeedsAck      bool            `json:"needsAck,omitempty"`
	HostOwner     string          `json:"hostOwner,omitempty"`
	PortOwner     string          `json:"portOwner,omitempty"`
	IsConsuming   bool            `json:"isConsuming,omitempty"`
	ConnHostOwner PignaConnection `json:"connHostOwner,omitempty"`
	ForwardConn   PignaConnection `json:"forwardConn,omitempty"`
	ClientConn    net.Conn        `json:"clientConn,omitempty"`
}

type Message struct {
	Body        string `json:"body,omitempty"`
	UUID        string `json:"UUID,omitempty"`
	IsAChunk    bool   `json:"isAChunk,omitempty"`
	NChunk      int    `json:"nChunk,omitempty"`
	TotalChunks int    `json:"totalChunks,omitempty"`
	MsgId       int    `json:"msgId,omitempty"`
}

var localQueueList map[string]*Queue
var senderName string

// here will be stored the message chunks waiting to be complete
// the key is the MsgUUID
var chunked map[string](map[int]Response)

func (req *Request) String() string {
	reqString, _ := json.Marshal(req)
	return string(reqString)
}

func Connect(hostname, port, sn string) (PignaConnection, error) {
	var pignaConn PignaConnection

	if chunked == nil {
		chunked = make(map[string](map[int]Response))
	}
	if localQueueList == nil {
		localQueueList = make(map[string]*Queue)
	}

	conn, err := net.Dial("tcp", hostname+":"+port)

	pignaConn.Connection = conn
	pignaConn.Hostname = hostname
	pignaConn.Port = port
	senderName = sn
	return pignaConn, err
}

func (pignaConn PignaConnection) Disconnect() {
	for {
		keepConsuming := false
		for _, queue := range localQueueList {
			if queue.IsConsuming {
				keepConsuming = true
			}
		}
		if !keepConsuming {
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}
	pignaConn.Connection.Close()
}

func (pignaConn PignaConnection) CheckQueueName(queueName string) (bool, error) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "checkQueueName",
		Queue: Queue{
			QueueName: queueName,
		},
	}

	_, isPresentLocally := localQueueList[queueName]

	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)

	if !res.ResponseTextBool {
		// delete local cache
		if isPresentLocally {
			delete(localQueueList, queueName)
		}
		return false, errors.New("No queue with this name")
	}
	return true, err
}

func (q Queue) GetNumberOfPaired() (int, error) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "getNumOfPaired",
		Queue:      q,
	}
	writeToClient(q.ConnHostOwner.Connection, req.String())
	res, err := waitForResponse(q.ConnHostOwner)
	return res.ResponseTextInt, err
}

func (q Queue) GetNumberOfUnacked() (int, error) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "getNumOfUnacked",
		Queue:      q,
	}
	writeToClient(q.ConnHostOwner.Connection, req.String())
	res, err := waitForResponse(q.ConnHostOwner)
	return res.ResponseTextInt, err
}

func (q Queue) GetNumberOfUnconsumed() (int, error) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "getNumOfUnconsumed",
		Queue:      q,
	}
	writeToClient(q.ConnHostOwner.Connection, req.String())
	res, err := waitForResponse(q.ConnHostOwner)
	return res.ResponseTextInt, err
}

func (q Queue) GetNamesOfPaired() ([]string, error) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "getNamesOfPaired",
		Queue:      q,
	}
	writeToClient(q.ConnHostOwner.Connection, req.String())
	res, err := waitForResponse(q.ConnHostOwner)
	stringSlice := strings.Split(res.ResponseTextString, ",")

	return stringSlice, err
}

func (pignaConn PignaConnection) GetQueueNames() ([]string, error) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "getQueueNames",
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)
	stringSlice := strings.Split(res.ResponseTextString, ",")

	return stringSlice, err
}

func (pignaConn PignaConnection) GetNumberOfQueues() (int, error) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "getNumOfQueues",
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)
	return res.ResponseTextInt, err
}

func CreateQueueStruct(queueName string) Queue {
	queueStruct := Queue{
		QueueName: queueName,
		QueueType: "normal",
		NeedsAck:  false,
	}
	return queueStruct
}

func (pignaConn PignaConnection) CreateQueue(q Queue) (Queue, error) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "createQueue",
		Queue:      q,
	}
	writeToClient(pignaConn.Connection, req.String())
	res, err := waitForResponse(pignaConn)

	if res.ResponseType == "success" {

		var newQueue Queue
		dec, _ := base64.StdEncoding.DecodeString(res.ResponseTextStringEncoded)
		json.Unmarshal([]byte(dec), &newQueue)
		localQueueList[q.QueueName] = &newQueue

		if newQueue.HostOwner == pignaConn.Hostname &&
			newQueue.PortOwner == pignaConn.Port {

			localQueueList[q.QueueName].ConnHostOwner = pignaConn
		} else {
			conn, err := Connect(newQueue.HostOwner, newQueue.PortOwner, senderName)
			if err != nil {
				delete(localQueueList, q.QueueName)
				return q, errors.New("Cannot connect to the host " +
					res.ResponseTextString)
			}
			localQueueList[q.QueueName].ConnHostOwner = conn
		}

		if q.ForwardConn.Connection == nil {
			localQueueList[q.QueueName].ForwardConn, err = Connect(localQueueList[q.QueueName].HostOwner, localQueueList[q.QueueName].ForwardConn.Port, senderName)
			if err != nil {
				return q, errors.New("Error connecting to the serverg")
			}
		}

	} else {
		return q, errors.New(res.ResponseTextString)
	}

	return *localQueueList[q.QueueName], err
}

func (q Queue) DestroyQueue() (Response, error) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "destroyQueue",
		Queue:      q,
	}
	writeToClient(q.ConnHostOwner.Connection, req.String())
	res, err := waitForResponse(q.ConnHostOwner)
	_, isPresent := localQueueList[q.QueueName]
	if isPresent {
		delete(localQueueList, q.QueueName)
	}
	return res, err
}

func (q Queue) ConsumeQueue(callback func(Queue, Response)) error {

	var req Request = Request{
		SenderName: senderName,
		Action:     "consumeQueue",
		Queue:      q,
	}

	_, isPresent := localQueueList[q.QueueName]
	if !isPresent {
		return errors.New("This queue does not exists locally")
	}

	writeToClient(q.ForwardConn.Connection, req.String())
	res, _ := waitForResponse(q.ForwardConn)

	if res.ResponseType == "success" {
		localQueueList[q.QueueName].IsConsuming = true
		go consume(q, callback)
	}
	return nil
}

func (q Queue) RemoveConsumer() (Response, error) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "removeConsumer",
		Queue:      q,
	}

	writeToClient(q.ConnHostOwner.Connection, req.String())
	res, err := waitForResponse(q.ConnHostOwner)

	_, isPresent := localQueueList[q.QueueName]
	if !isPresent {
		return res, errors.New("This queue does not exists locally")
	}
	if res.ResponseType == "success" && localQueueList[q.QueueName].IsConsuming {
		localQueueList[q.QueueName].IsConsuming = false
	}
	return res, err
}

func (q Queue) HasBeenAcked(messageUUID uuid.UUID) (bool, error) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "hasBeenAcked",
		Queue:      q,
		Message: Message{
			UUID: messageUUID.String(),
		},
	}
	writeToClient(q.ConnHostOwner.Connection, req.String())
	res, err := waitForResponse(q.ConnHostOwner)
	return res.ResponseTextBool, err
}

func (q Queue) SendMsg(message string) uuid.UUID {
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
			SenderName: senderName,
			Action:     "sendMsg",
			Queue: Queue {
				QueueName: q.QueueName,
				QueueType: q.QueueType,
				NeedsAck: q.NeedsAck,
			},
			Message: Message{
				Body: encodedMessage,
				UUID: u1.String(),
			},
		}

		writeToClient(q.ForwardConn.Connection, req.String())
		return u1
	}

	var req Request = Request{
		SenderName: senderName,
		Action:     "sendMsg",
		Queue: Queue {
			QueueName: q.QueueName,
			QueueType: q.QueueType,
			NeedsAck: q.NeedsAck,
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
		writeToClient(q.ForwardConn.Connection, req.String())
	}
	return u1
}

func consume(q Queue, callback func(Queue, Response)) {
	chunkSize := 1024
	broken := ""
	for localQueueList[q.QueueName].IsConsuming {
		var response Response

		var buffer = make([]byte, chunkSize)
		readLen, err := q.ForwardConn.Connection.Read(buffer)

		if err != nil {
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
					ackMessage(q.ForwardConn.Connection, response)
				}
				callback(q, response)
			}
		}
	}
}

func ackMessage(conn net.Conn, res Response) {
	var req Request = Request{
		SenderName: senderName,
		Action:     "msgAck",
		Queue: Queue{
			QueueName: res.QueueName,
		},
		Message: Message{
			UUID: res.MsgUUID,
		},
	}
	writeToClient(conn, req.String())
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
