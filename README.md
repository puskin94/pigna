# Pigna


<img src="http://puskin.it/pigna_logo_blue_dark.png" height="200" width="250">

## What is Pigna
*Pigna* is a software written in Go capable of managing and routing messages from Producers to Consumers.

## How does it works?
Producers and Consumers communicate thanks to a daemon called (to be creative) **pignaDaemon**; just run it (`go run daemon/pignaDaemon.go`) and you are ready to go. It listens on the port number **16789** but you can change it specifying the `--port` param.

If on a queue there are only publishers and no consumers, all the sent messages are stored by the daemon and sent when a consumer `Pairs` to a queue.

### config.json
This file is mandatory because it contains useful informations:

* the client name

TO ADD
```

the server ip (TODO. Now this information is provided through the commandline)

the client authentication (TODO)

```

### Queues
Every message is routed on *queues* and on every *queue* are listening one or more consumers. Every consumer receives, unmarshall and handles the message

#### Ack
*Queues* can be created with the ack option: if so, every consumer, every time receives a message warns the **pignaDaemon** about the successfull transmission.

## The Pigna Lib (APIs)

Message communication is made easy thanks to the apis provided by the **pigna** lib:

#### Connect(host string, port string, configFilename string) (PignaConnection, error)
It creates the connection to the daemon and returns the object mandatory for all the further actions

#### (pignaConn PignaConnection) Disconnect()
It closes the connection to the daemon

#### (pignaConn PignaConnection) GetNumberOfPaired(queueName string)
Returns the number of consumers on a secified queue

#### (pignaConn PignaConnection) GetNumberOfUnacked(queueName string)
If the queue has been created with the *ack* flag, it returns the number of messages not already acked.

#### (pignaConn PignaConnection) GetNumberOfUnconsumed(queueName string)
Returns the number of unconsumed messages on a specific queue

#### (pignaConn PignaConnection) GetNamesOfPaired(queueName string)
Returns an array containing the names of the client that are consuming a queue

#### (pignaConn PignaConnection) GetQueueNames()
Returns an array containing all the existing queues

### CreateQueueStruct (queueName string) Queue
The `Queue` struct is having multiple changes. Due to this you need to call `pigna.CreateQueueStruct` to get a basic `Queue` config. By default it sets the `NeedsAck` to false and `QueueType` to normal. You can change the `QueueType` value to these values:

- normal : all the messages will be sent in broadcast to all the Consumers
- loadBalanced : every message will be routed to Consumers using a Round Robin algorithm

#### (pignaConn PignaConnection) CreateQueue(queue Queue)
This function creates a *Queue*. You need to specify the `Queue` struct that contains infos about the `Queue` type.

#### (pignaConn PignaConnection) DestroyQueue(queueName string)
It destroys the queue from the daemon

#### (pignaConn *PignaConnection) ConsumeQueue(queueName string, callback func(PignaConnection, Response))
Consuming a queue leads to specify a custom function that handles the messages flowing on the queue.
This is a working example:

```
func main() {
  pignaConn, err := pigna.Connect(hostname, "16789", "config.json")
  if err != nil {
    log.Println(err.Error())
    return
  }

  pignaConn.ConsumeQueue("queueTestName", msgHandler)
}



func msgHandler(pignaConn pigna.PignaConnection, msg pigna.Response) {
  log.Println(msg.MsgId)
}
```

#### (pignaConn *PignaConnection) RemoveConsumer(queueName string)
After consuming a queue, you need to destroy your connection from the daemon

#### (pignaConn PignaConnection) SendMsg(queueName string, message string) (uuid.UUID)
This function allows you to send messages through a Pigna queue. Just specify the `queueName` and the message.

#### (pignaConn PignaConnection) HasBeenAcked(queueName string, messageUUID uuid.UUID) (bool, error)
Given an `uuid.UUID` it returns if a message has been acked or not
