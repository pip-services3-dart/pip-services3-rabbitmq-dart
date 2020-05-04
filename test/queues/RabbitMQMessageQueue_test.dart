// package test_queues

// import (
// 	"os"
// 	"testing"

// 	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
// 	rabbitqueue "github.com/pip-services3-go/pip-services3-rabbitmq-go/queues"
// )

// func TestRabbitMQMessageQueue(t *testing.T) {

// 	var queue *rabbitqueue.RabbitMQMessageQueue
// 	var fixture *MessageQueueFixture

// 	rabbitmqHost := os.Getenv("RABBITMQ_HOST")
// 	if rabbitmqHost == "" {
// 		rabbitmqHost = "localhost"
// 	}
// 	rabbitmqPort := os.Getenv("RABBITMQ_PORT")
// 	if rabbitmqPort == "" {
// 		rabbitmqPort = "5672"
// 	}

// 	rabbitmqExchange := os.Getenv("RABBITMQ_EXCHANGE")
// 	if rabbitmqExchange == "" {
// 		rabbitmqExchange = "test"
// 	}

// 	rabbitmqQueue := os.Getenv("RABBITMQ_QUEUE")
// 	if rabbitmqQueue == "" {
// 		rabbitmqQueue = "test"
// 	}

// 	rabbitmqUser := os.Getenv("RABBITMQ_USER")
// 	if rabbitmqUser == "" {
// 		rabbitmqUser = "user"
// 	}

// 	rabbitmqPassword := os.Getenv("RABBITMQ_PASS")
// 	if rabbitmqPassword == "" {
// 		rabbitmqPassword = "password"
// 	}

// 	if rabbitmqHost == "" && rabbitmqPort == "" {
// 		return
// 	}

// 	queueConfig := cconf.NewConfigParamsFromTuples(
// 		"exchange", rabbitmqExchange,
// 		"queue", rabbitmqQueue,
// 		"options.auto_create", true,
// 		//"connection.protocol", "amqp",
// 		"connection.host", rabbitmqHost,
// 		"connection.port", rabbitmqPort,
// 		//"credential.username", rabbitmqUser,
// 		//"credential.password", rabbitmqPassword,
// 	)

// 	queue = rabbitqueue.NewEmptyRabbitMQMessageQueue("testQueue")
// 	queue.Configure(queueConfig)

// 	fixture = NewMessageQueueFixture(queue)

// 	qOpnErr := queue.Open("")
// 	if qOpnErr == nil {
// 		queue.Clear("")
// 	}

// 	defer queue.Close("")

// 	t.Run("RabbitMQMessageQueue:Send Receive Message", fixture.TestSendReceiveMessage)
// 	queue.Clear("")
// 	t.Run("RabbitMQMessageQueue:Receive Send Message", fixture.TestReceiveSendMessage)
// 	queue.Clear("")
// 	t.Run("RabbitMQMessageQueue:Receive And Complete Message", fixture.TestReceiveCompleteMessage)
// 	queue.Clear("")
// 	t.Run("RabbitMQMessageQueue:Receive And Abandon Message", fixture.TestReceiveAbandonMessage)
// 	queue.Clear("")
// 	t.Run("RabbitMQMessageQueue:Send Peek Message", fixture.TestSendPeekMessage)
// 	queue.Clear("")
// 	t.Run("RabbitMQMessageQueue:Peek No Message", fixture.TestPeekNoMessage)
// 	queue.Clear("")
// 	t.Run("RabbitMQMessageQueue:On Message", fixture.TestOnMessage)

// }
