package pigna

import (
	"testing"
	"github.com/satori/go.uuid"
)


func TestPigna(t *testing.T) {
	pignaConn, err := Connect("127.0.0.1", "16789", "senderTestName")
	if err != nil {
		t.Error(err.Error())
		return
	}

	exists, _ := pignaConn.CheckQueueName("queueTestName")
	if exists {
		t.Error("This queue should not exist!")
		return
	}

	queueStruct := CreateQueueStruct("queueTestName")
	// queueStruct.NeedsAck = true
	queue, err := pignaConn.CreateQueue(queueStruct)
	if err != nil {
		t.Error(err.Error())
		return
	}

	// consuming before publishing
	err = queue.ConsumeQueue(msgHandler)
	if err != nil {
		t.Error(err.Error())
		return
	}

	msgNum := 2
	var msgUUIDs []uuid.UUID
	for i := 0; i < msgNum; i++ {
		u := queue.SendMsg("test String To 256B")
		msgUUIDs = append(msgUUIDs, u)
	}

	for _, uu := range msgUUIDs {
		ack, _ := queue.HasBeenAcked(uu)
		if !ack {
			t.Error("Msg " + uu.String() + " has not been Acked!")
			return
		}
	}

	msgUUIDs = nil

	queue.RemoveConsumer()

	queue, err = pignaConn.CreateQueue(queueStruct)

	// publishing before consuming
	msgNum = 2
	for i := 0; i < msgNum; i++ {
		u := queue.SendMsg("test String To 256B")
		msgUUIDs = append(msgUUIDs, u)
	}

	err = queue.ConsumeQueue(msgHandler)
	if err != nil {
		t.Error(err.Error())
		return
	}

	for _, uu := range msgUUIDs {
		ack, _ := queue.HasBeenAcked(uu)
		if !ack {
			t.Error("Msg " + uu.String() + " has not been Acked!")
			return
		}
	}

	queue.RemoveConsumer()
	pignaConn.Disconnect()
}

func msgHandler(queue Queue, msg Response) {}
