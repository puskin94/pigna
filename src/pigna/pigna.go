package pigna


import (
	"net"
	"bufio"
	"log"
	"time"
	"encoding/json"
)

type PignaConnection struct {
	Connection net.Conn
	IsConsuming bool
}

type Response struct {
	ResponseType string `json:"responseType"`
	ResponseText string `json:"responseText"`
	QueueName string `json:"queueName"`
}

func Connect(host string, port string) (PignaConnection, error) {
	var pignaConn PignaConnection
	conn, err := net.Dial("tcp", host + ":" + port)
	pignaConn.Connection = conn
	pignaConn.IsConsuming = false
	return pignaConn, err
}

func(pignaConn PignaConnection) Disconnect() {
	log.Println("Pigna will quit when stop consuming...")
	for pignaConn.IsConsuming {
		time.Sleep(1000 * time.Millisecond)
	}
	pignaConn.Connection.Close()
}

func(pignaConn PignaConnection) CheckQueueName(queueName string) (Response, error) {
	checkQueueName := `{"action":"checkQueueName","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, checkQueueName)
	res, err := waitForResponse(pignaConn)
	return res, err
}

func(pignaConn PignaConnection) CreateQueue(queueName string) (Response, error) {
	createQueue := `{"action":"createQueue","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, createQueue)
	res, err := waitForResponse(pignaConn)
	return res, err
}

func(pignaConn PignaConnection) DestroyQueue(queueName string) (Response, error) {
	destroyQueue := `{"action":"destroyQueue","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, destroyQueue)
	res, err := waitForResponse(pignaConn)
	return res, err
}

func(pignaConn *PignaConnection) ConsumeQueue(queueName string, callback func(Response)) {
	consumeQueue := `{"action":"consumeQueue","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, consumeQueue)

	pignaConn.IsConsuming = true
	go consume(*pignaConn, callback)
}

func(pignaConn *PignaConnection) RemoveConsumer(queueName string) (Response, error) {
	removeConsumer := `{"action":"removeConsumer","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, removeConsumer)
	res, err := waitForResponse(*pignaConn)
	// stop the consuming go routine
	pignaConn.IsConsuming = false
	return res, err
}

func(pignaConn PignaConnection) SendMsg(queueName string, message string) {
	sendMsg := `{"action":"sendMsg","queue":{"queueName":"` +
		queueName + `"}, "message": {"body": "`+ message + `"}}`
	writeToClient(pignaConn.Connection, sendMsg)
}

func consume(pignaConn PignaConnection, callback func(Response)) {
	for pignaConn.IsConsuming {
		var response Response
		message, err := bufio.NewReader(pignaConn.Connection).ReadString('\n')
		if err != nil {
			log.Println(err.Error())
		}
		_ = json.Unmarshal([]byte(message), &response)
		callback(response)
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
