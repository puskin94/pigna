package pigna


import (
	"net"
	"bufio"
	"encoding/json"
)

type PignaConnection struct {
	Connection net.Conn
}

type Response struct {
	ResponseType string `json:"responseType"`
	ResponseText string `json:"responseText"`
}

func Connect(host string, port string) (PignaConnection, error) {
	var pignaConn PignaConnection
	conn, err := net.Dial("tcp", host + ":" + port)
	pignaConn.Connection = conn
	return pignaConn, err
}

func(pignaConn PignaConnection) Disconnect() {
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

func(pignaConn PignaConnection) ConsumeQueue(queueName string) (Response, error) {
	createQueue := `{"action":"consumeQueue","queue":{"queueName":"` +
					queueName + `"}}`
	writeToClient(pignaConn.Connection, createQueue)
	res, err := waitForResponse(pignaConn)

	// XXX: start a listening goroutine unless `RemoveConsumer`

	return res, err
}

func(pignaConn PignaConnection) RemoveConsumer(queueName string) (Response, error) {
	createQueue := `{"action":"removeConsumer","queue":{"queueName":"` +
					queueName + `"}}`
	writeToClient(pignaConn.Connection, createQueue)
	res, err := waitForResponse(pignaConn)
	return res, err
}

// XXX: add the timestamp to the message!
func(pignaConn PignaConnection) SendMsg(queueName string, message string) {
	createQueue := `{"action":"sendMsg","queue":{"queueName":"` +
					queueName + `"}, "message": {"body": "`+ message + `"}}`
	writeToClient(pignaConn.Connection, createQueue)
	// res, err := waitForResponse(pignaConn)
	// return res, err
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
