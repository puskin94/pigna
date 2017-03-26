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

	var response Response
	message, _ := bufio.NewReader(pignaConn.Connection).ReadString('\n')
	err := json.Unmarshal([]byte(message), &response)

	return response, err
}

func writeToClient(conn net.Conn, message string) {
	conn.Write([]byte(message + "\n"))
}
