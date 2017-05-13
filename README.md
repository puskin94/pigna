# Pigna


<img src="http://puskin.it/pigna_logo_blue_dark.png" height="200" width="250">

## What is Pigna
*Pigna* is a software written in Go capable of managing and routing messages from Producers to Consumers.

## How does it works?
Producers and Consumers communicate thanks to a daemon called (to be creative) **pignaDaemon**; just run it (`go run daemon/pignaDaemon.go`) and you are ready to go. It listens on the port number **16789** but you can change it specifying the `--port` param.

On this port the client will perform basic operations that will not involve consuming or publishing messages. For that type of actions the `pignaDaemon` will expose a random free port where to send messages.

If on a queue there are only publishers and no consumers, all the sent messages are stored by the daemon and sent when a consumer `Pairs` to a queue.

## Clustering
Is possible to add another `pignaDaemon` with the flag `--clusterHost=<main pigna daemon ip>:<main pigna daemon port>` to create another instance that can handle queues too. <br>
The `Pigna` client will create a connection to the main daemon and new queues will be created on the daemon which contains less; if the daemon is a *clustered instance* the client will use that connection to send and receive messages.


### Queues
Every message is routed on *queues* and on every *queue* are listening one or more consumers. Every consumer receives, unmarshall and handles the message

#### Ack
*Queues* can be created with the ack option: if so, every consumer, every time receives a message warns the **pignaDaemon** about the successfull transmission.

## The Pigna Lib (APIs)

Message communication is made easy thanks to the apis provided by the **pigna** lib:

#### Connect(hostname, port, senderName string) (PignaConnection, error)
It creates the connection to the daemon and returns the object mandatory for all the further actions.

#### (pignaConn PignaConnection) Disconnect()
It closes the connection to the daemon

#### (pignaConn PignaConnection) CheckQueueName(queueName string) (bool, error)
It returns true if the `queueName` exists, false if not

#### (pignaConn PignaConnection) CreateQueue(queueStruct Queue) (Queue, error)
This function creates a *Queue*. You need to specify the `Queue` struct that contains infos about the `Queue` type. If the queue already exists, it returns a `Queue` struct ready to be consumed or to publish in.

#### (pignaConn PignaConnection) GetQueueNames() ([]string, error)
Returns an array containing all the existing queues

#### (q Queue) GetNumberOfPaired() (int, error)
Returns the number of consumers on a secified queue

#### (q Queue) GetNumberOfUnacked() (int, error)
If the queue has been created with the *ack* flag, it returns the number of messages not already acked.

#### (q Queue) GetNumberOfUnconsumed() (int, error)
Returns the number of unconsumed messages on a specific queue

#### (q Queue) GetNamesOfPaired() ([]string, error)
Returns an array containing the names of the client that are consuming a queue

#### CreateQueueStruct (queueName string) (Queue)
The `Queue` struct is having multiple changes. Due to this you need to call `pigna.CreateQueueStruct` to get a basic `Queue` config. By default it sets the `NeedsAck` to false and `QueueType` to normal. You can change the `QueueType` value to these values:

- normal : all the messages will be sent in broadcast to all the Consumers
- roundRobin : every message will be routed to Consumers using a Round Robin algorithm

#### (q Queue) DestroyQueue()
It destroys the queue from the daemon

#### (q Queue) ConsumeQueue(callback func(Queue, Response)) (error)
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

#### (q Queue) RemoveConsumer() (Response, error)
After consuming a queue, you need to destroy your connection from the daemon

#### (q Queue) SendMsg(message string) (uuid.UUID)
This function allows you to send messages through a Pigna queue. Just specify the `queueName` and the message.

#### (q Queue) HasBeenAcked(messageUUID uuid.UUID) (bool, error)
Given an `uuid.UUID` it returns if a message has been acked or not


### Changelog

#### 0.0.8
Added basic testing

Sending and Consuming are now on a different port

Changed from "loadBalanced" to "roundRobin"
