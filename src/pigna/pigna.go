package pigna


import (
	"net"
	"bufio"
	"log"
	"time"
	"encoding/json"
)

// var wg sync.WaitGroup

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

func(pignaConn PignaConnection) CreateQueue(queueName string) (Response, error) {
	createQueue := `{"action":"createQueue","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, createQueue)
	res, err := waitForResponse(pignaConn)
	return res, err
}

func(pignaConn PignaConnection) DestroyQueue(queueName string) (Response, error) {
	createQueue := `{"action":"destroyQueue","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, createQueue)
	res, err := waitForResponse(pignaConn)
	return res, err
}

func(pignaConn *PignaConnection) ConsumeQueue(queueName string, callback func(Response)) {
	createQueue := `{"action":"consumeQueue","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, createQueue)

	pignaConn.IsConsuming = true
	go consume(*pignaConn, callback)
}

func(pignaConn *PignaConnection) RemoveConsumer(queueName string) (Response, error) {
	createQueue := `{"action":"removeConsumer","queue":{"queueName":"` +
		queueName + `"}}`
	writeToClient(pignaConn.Connection, createQueue)
	res, err := waitForResponse(*pignaConn)
	// stop the consuming go routine
	pignaConn.IsConsuming = false
	return res, err
}

// XXX: add the timestamp to the message!
func(pignaConn PignaConnection) SendMsg(queueName string, message string) {
	now := time.Now()
	createQueue := `{"action":"sendMsg","queue":{"queueName":"` +
		queueName + `"}, "message": {"body": "`+ message +
		`", "timestamp": "` + now.Format("20060102150405") + `"}}`
	writeToClient(pignaConn.Connection, createQueue)
}

func consume(pignaConn PignaConnection, callback func(Response)) {
	for pignaConn.IsConsuming {
		var response Response
		message, err := bufio.NewReader(pignaConn.Connection).ReadString('\n')
		if err != nil {
			log.Println(err.Error())
		}
		// log.Println(message)
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
