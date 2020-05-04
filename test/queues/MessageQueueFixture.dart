// package test_queues

// import (
// 	"fmt"
// 	"testing"
// 	"time"

// 	"github.com/pip-services3-go/pip-services3-messaging-go/queues"
// 	"github.com/stretchr/testify/assert"
// )

// type MessageQueueFixture struct {
// 	queue queues.IMessageQueue
// }

// func NewMessageQueueFixture(queue queues.IMessageQueue) *MessageQueueFixture {
// 	mqf := MessageQueueFixture{}
// 	mqf.queue = queue
// 	return &mqf
// }

// func (c *MessageQueueFixture) TestSendReceiveMessage(t *testing.T) {
// 	envelope1 := queues.NewMessageEnvelope("123", "Test", "Test message")
// 	var envelope2 *queues.MessageEnvelope

// 	sndErr := c.queue.Send("", envelope1)
// 	assert.Nil(t, sndErr)

// 	count, rdErr := c.queue.ReadMessageCount()
// 	assert.Nil(t, rdErr)
// 	assert.Greater(t, count, (int64)(0))

// 	result, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
// 	envelope2 = result
// 	assert.Nil(t, rcvErr)
// 	assert.NotNil(t, envelope2)
// 	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
// 	assert.Equal(t, envelope1.Message, envelope2.Message)
// 	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

// }

// func (c *MessageQueueFixture) TestMessageCount(t *testing.T) {
// 	envelope1 := queues.NewMessageEnvelope("123", "Test", "Test message")
// 	c.queue.Send("", envelope1)

// 	select {
// 	case <-time.After(500 * time.Millisecond):
// 	}

// 	count, err := c.queue.ReadMessageCount()
// 	assert.Nil(t, err)
// 	assert.True(t, count >= 1)

// }

// func (c *MessageQueueFixture) TestReceiveSendMessage(t *testing.T) {
// 	envelope1 := queues.NewMessageEnvelope("123", "Test", "Test message")
// 	var envelope2 *queues.MessageEnvelope

// 	time.AfterFunc(500*time.Millisecond, func() {
// 		c.queue.Send("", envelope1)
// 	})

// 	result, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
// 	envelope2 = result
// 	assert.Nil(t, rcvErr)
// 	assert.NotNil(t, envelope2)
// 	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
// 	assert.Equal(t, envelope1.Message, envelope2.Message)
// 	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

// }

// func (c *MessageQueueFixture) TestReceiveCompleteMessage(t *testing.T) {
// 	envelope1 := queues.NewMessageEnvelope("123", "Test", "Test message")
// 	var envelope2 *queues.MessageEnvelope

// 	sndErr := c.queue.Send("", envelope1)
// 	assert.Nil(t, sndErr)

// 	count, rdErr := c.queue.ReadMessageCount()
// 	assert.Nil(t, rdErr)
// 	assert.Greater(t, count, (int64)(0))

// 	result, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
// 	assert.Nil(t, rcvErr)
// 	envelope2 = result

// 	assert.NotNil(t, envelope2)
// 	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
// 	assert.Equal(t, envelope1.Message, envelope2.Message)
// 	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

// 	cplErr := c.queue.Complete(envelope2)
// 	assert.Nil(t, cplErr)
// 	assert.Nil(t, envelope2.GetReference())

// }

// func (c *MessageQueueFixture) TestReceiveAbandonMessage(t *testing.T) {
// 	envelope1 := queues.NewMessageEnvelope("123", "Test", "Test message")
// 	var envelope2 *queues.MessageEnvelope

// 	sndErr := c.queue.Send("", envelope1)
// 	assert.Nil(t, sndErr)

// 	result, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
// 	assert.Nil(t, rcvErr)
// 	envelope2 = result

// 	assert.NotNil(t, envelope2)
// 	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
// 	assert.Equal(t, envelope1.Message, envelope2.Message)
// 	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

// 	abdErr := c.queue.Abandon(envelope2)
// 	assert.Nil(t, abdErr)

// 	result, rcvErr = c.queue.Receive("", 10000*time.Millisecond)
// 	assert.Nil(t, rcvErr)
// 	envelope2 = result

// 	assert.NotNil(t, envelope2)
// 	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
// 	assert.Equal(t, envelope1.Message, envelope2.Message)
// 	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

// }

// func (c *MessageQueueFixture) TestSendPeekMessage(t *testing.T) {
// 	envelope1 := queues.NewMessageEnvelope("123", "Test", "Test message")
// 	var envelope2 *queues.MessageEnvelope

// 	sndErr := c.queue.Send("", envelope1)
// 	assert.Nil(t, sndErr)

// 	result, pkErr := c.queue.Peek("")
// 	assert.Nil(t, pkErr)
// 	envelope2 = result

// 	assert.NotNil(t, envelope2)
// 	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
// 	assert.Equal(t, envelope1.Message, envelope2.Message)
// 	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)
// 	// pop message from queue for next test
// 	_, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
// 	assert.Nil(t, rcvErr)

// }

// func (c *MessageQueueFixture) TestPeekNoMessage(t *testing.T) {
// 	result, pkErr := c.queue.Peek("")
// 	assert.Nil(t, pkErr)
// 	assert.Nil(t, result)

// }

// func (c *MessageQueueFixture) TestMoveToDeadMessage(t *testing.T) {
// 	envelope1 := queues.NewMessageEnvelope("123", "Test", "Test message")
// 	var envelope2 *queues.MessageEnvelope

// 	sndErr := c.queue.Send("", envelope1)
// 	assert.Nil(t, sndErr)

// 	result, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
// 	assert.Nil(t, rcvErr)
// 	envelope2 = result
// 	assert.NotNil(t, envelope2)
// 	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
// 	assert.Equal(t, envelope1.Message, envelope2.Message)
// 	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

// 	mvErr := c.queue.MoveToDeadLetter(envelope2)
// 	assert.Nil(t, mvErr)

// }

// func (c *MessageQueueFixture) TestOnMessage(t *testing.T) {
// 	envelope1 := queues.NewMessageEnvelope("123", "Test", "Test message")
// 	var envelope2 *queues.MessageEnvelope
// 	var reciver TestMsgReciver = TestMsgReciver{}
// 	c.queue.BeginListen("", &reciver)

// 	select {
// 	case <-time.After(1000 * time.Millisecond):
// 	}

// 	sndErr := c.queue.Send("", envelope1)
// 	assert.Nil(t, sndErr)

// 	select {
// 	case <-time.After(1000 * time.Millisecond):
// 	}

// 	envelope2 = reciver.envelope

// 	assert.NotNil(t, envelope2)
// 	assert.Equal(t, envelope1.Message_type, envelope2.Message_type)
// 	assert.Equal(t, envelope1.Message, envelope2.Message)
// 	assert.Equal(t, envelope1.Correlation_id, envelope2.Correlation_id)

// 	c.queue.EndListen("")

// 	fmt.Println("End test")

// }

// type TestMsgReciver struct {
// 	envelope *queues.MessageEnvelope
// }

// func (c *TestMsgReciver) ReceiveMessage(envelope *queues.MessageEnvelope, queue queues.IMessageQueue) (err error) {
// 	c.envelope = envelope
// 	return nil
// }
