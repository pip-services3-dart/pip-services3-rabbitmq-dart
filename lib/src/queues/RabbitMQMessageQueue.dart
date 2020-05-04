// package queues

// import (
// 	"sync"
// 	"time"

// 	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
// 	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
// 	cauth "github.com/pip-services3-go/pip-services3-components-go/auth"
// 	ccon "github.com/pip-services3-go/pip-services3-components-go/connect"
// 	msgqueues "github.com/pip-services3-go/pip-services3-messaging-go/queues"
// 	mqcon "github.com/pip-services3-go/pip-services3-rabbitmq-go/connect"
// 	rabbitmq "github.com/streadway/amqp"
// )

// /*
//    Message queue that sends and receives messages via MQTT message broker.

//    MQTT is a popular light-weight protocol to communicate IoT devices.

//    Configuration parameters:

//    - topic:                         name of MQTT topic to subscribe

//    connection(s):
//    - discovery_key:               (optional) a key to retrieve the connection from <a href="https://rawgit.com/pip-services3-dotnet/pip-services3-components-dotnet/master/doc/api/interface_pip_services_1_1_components_1_1_connect_1_1_i_discovery.html IDiscovery</a>
//    - host:                        host name or IP address
//    - port:                        port number
//    - uri:                         resource URI or connection string with all parameters in it

//    credential(s):
//    - store_key:                   (optional) a key to retrieve the credentials from <a href="https://rawgit.com/pip-services3-dotnet/pip-services3-components-dotnet/master/doc/api/interface_pip_services_1_1_components_1_1_auth_1_1_i_credential_store.html ICredentialStore</a>
//    - username:                    user name
//    - password:                    user password

//    References:

//    - *:logger:*:*:1.0             (optional) <a href="https://rawgit.com/pip-services3-dotnet/pip-services3-components-dotnet/master/doc/api/interface_pip_services_1_1_components_1_1_log_1_1_i_logger.html ILogger</a> components to pass log messages
//    - *:counters:*:*:1.0           (optional) <a href="https://rawgit.com/pip-services3-dotnet/pip-services3-components-dotnet/master/doc/api/interface_pip_services_1_1_components_1_1_count_1_1_i_counters.html ICounters</a> components to pass collected measurements
//    - *:discovery:*:*:1.0          (optional) <a href="https://rawgit.com/pip-services3-dotnet/pip-services3-components-dotnet/master/doc/api/interface_pip_services_1_1_components_1_1_connect_1_1_i_discovery.html IDiscovery</a> services to resolve connections
//    - *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials



//    var queue = new RabbitMQMessageQueue("myqueue");
//    queue.configure(ConfigParams.FromTuples(
//    "topic", "mytopic",
//    "connection.protocol", "mqtt"
//    "connection.host", "localhost"
//    "connection.port", 1883 ));
//    queue.Open("123");

//    queue.Send("123", new MessageEnvelop(null, "mymessage", "ABC"));
//    queue.Receive("123", 0);
//    queue.Complete("123", message);
// */

// type RabbitMQMessageQueue struct {
// 	*msgqueues.MessageQueue
// 	defaultCheckInterval int64
// 	connection           *rabbitmq.Connection
// 	mqChanel             *rabbitmq.Channel
// 	optionsResolver      *mqcon.RabbitMQConnectionResolver
// 	queue                string
// 	exchange             string
// 	exchangeType         string
// 	routingKey           string
// 	persistent           bool
// 	exclusive            bool
// 	autoCreate           bool
// 	autoDelete           bool
// 	noQueue              bool
// 	cancel               chan bool
// 	Interval             time.Duration
// }

// //  Creates a new instance of the message queue.
// //  name(optional) a queue name.
// func NewEmptyRabbitMQMessageQueue(name string) *RabbitMQMessageQueue {

// 	c := RabbitMQMessageQueue{
// 		defaultCheckInterval: 1000,
// 		exchange:             "",
// 		exchangeType:         "fanout",
// 		routingKey:           "",
// 		persistent:           false,
// 		exclusive:            false,
// 		autoCreate:           false,
// 		autoDelete:           false,
// 		noQueue:              false,
// 		cancel:               nil,
// 	}

// 	c.MessageQueue = msgqueues.NewMessageQueue(name)
// 	c.MessageQueue.IMessageQueue = &c
// 	c.Capabilities = msgqueues.NewMessagingCapabilities(true, true, true, true, true, false, true, false, true)
// 	c.Interval = time.Duration(c.defaultCheckInterval) * time.Millisecond
// 	c.optionsResolver = mqcon.NewRabbitMQConnectionResolver()
// 	return &c
// }

// func NewRabbitMQMessageQueueFromConfig(name string, config *cconf.ConfigParams) *RabbitMQMessageQueue {

// 	c := NewEmptyRabbitMQMessageQueue(name)
// 	if config != nil {
// 		c.Configure(config)
// 	}
// 	return c
// }

// func NewRabbitMQMessageQueue(name string, mqChanel *rabbitmq.Channel, queue string) *RabbitMQMessageQueue {

// 	c := NewEmptyRabbitMQMessageQueue(name)
// 	c.mqChanel = mqChanel
// 	c.queue = queue
// 	return c
// }

// //  Configures component by passing configuration parameters.
// //  - config configuration parameters to be set.
// func (c *RabbitMQMessageQueue) Configure(config *cconf.ConfigParams) {
// 	c.MessageQueue.Configure(config)

// 	c.Interval = time.Duration(config.GetAsLongWithDefault("interval", int64(c.defaultCheckInterval))) * time.Millisecond

// 	c.queue = config.GetAsStringWithDefault("queue", c.queue)
// 	c.exchange = config.GetAsStringWithDefault("exchange", c.exchange)

// 	c.exchangeType = config.GetAsStringWithDefault("options.exchange_type", c.exchangeType)
// 	c.routingKey = config.GetAsStringWithDefault("options.routing_key", c.routingKey)
// 	c.persistent = config.GetAsBooleanWithDefault("options.persistent", c.persistent)
// 	c.exclusive = config.GetAsBooleanWithDefault("options.exclusive", c.exclusive)
// 	c.autoCreate = config.GetAsBooleanWithDefault("options.auto_create", c.autoCreate)
// 	c.autoDelete = config.GetAsBooleanWithDefault("options.auto_delete", c.autoDelete)
// 	c.noQueue = config.GetAsBooleanWithDefault("options.noqueue", c.noQueue)
// }

// func (c *RabbitMQMessageQueue) checkOpened(String correlationId) error {
// 	if c.mqChanel == nil {
// 		return cerr.NewInvalidStateError(correlationId, "NOT_OPENED", "The queue is not opened")
// 	}
// 	return nil
// }

// //  Checks if the component is opened.
// //  Retruns : true if the component has been opened and false otherwise.
// func (c *RabbitMQMessageQueue) IsOpen() bool {
// 	return c.connection != nil && c.mqChanel != nil
// }

// //  Opens the component with given connection and credential parameters.
// //  - correlationId (optional) transaction id to trace execution through call chain.
// //  - connection connection parameters
// //  - credential credential parameters
// func (c *RabbitMQMessageQueue) OpenWithParams(String correlationId, connection *ccon.ConnectionParams, credential *cauth.CredentialParams) error {

// 	options, err := c.optionsResolver.Compose(correlationId, connection, credential)
// 	if err != nil {
// 		return err
// 	}

// 	if c.queue == "" && c.exchange == "" {
// 		return cerr.NewConfigError(correlationId,
// 			"NO_QUEUE",
// 			"Queue or exchange are not defined in connection parameters")
// 	}

// 	conn, err := rabbitmq.Dial(options.Get("uri"))
// 	if err != nil {
// 		return err
// 	}
// 	c.connection = conn
// 	c.mqChanel, err = conn.Channel()
// 	if err != nil {
// 		return err
// 	}
// 	//c.cancel = make(chan bool)

// 	// Automatically create queue, exchange and binding
// 	if c.autoCreate {
// 		if c.exchange != "" {
// 			c.mqChanel.ExchangeDeclare(
// 				c.exchange,
// 				c.exchangeType,
// 				c.persistent,
// 				c.autoDelete,
// 				false,
// 				false,
// 				nil,
// 			)
// 		}

// 		if !c.noQueue {

// 			if c.queue == "" {
// 				res, err := c.mqChanel.QueueDeclare(
// 					"",
// 					c.persistent,
// 					true,
// 					true,
// 					false,
// 					nil,
// 				)
// 				if err != nil {
// 					return err
// 				}
// 				c.queue = res.Name
// 			} else {
// 				c.mqChanel.QueueDeclare(
// 					c.queue,
// 					c.persistent,
// 					c.exclusive,
// 					c.autoDelete,
// 					false,
// 					nil,
// 				)
// 			}

// 			c.mqChanel.QueueBind(
// 				c.queue,
// 				c.routingKey,
// 				c.exchange,
// 				false,
// 				nil,
// 			)

// 		}
// 	}
// 	return nil
// }

// // Close mwthod are closes component and frees used resources.
// //  Parameters:
// //   - correlationId (optional) transaction id to trace execution through call chain.
// func (c *RabbitMQMessageQueue) Close(String correlationId) (err error) {

// 	if c.cancel != nil {
// 		c.cancel <- true
// 		c.cancel = nil
// 	}

// 	if c.mqChanel != nil {
// 		err = c.mqChanel.Close()
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	if c.connection != nil {
// 		err = c.connection.Close()
// 	}
// 	c.connection = nil
// 	c.mqChanel = nil
// 	c.Logger.Trace(correlationId, "Closed queue %s", c.queue)
// 	return err
// }

// // ReadMessageCount method are reads the current number of messages in the queue to be delivered.
// // Returns count int64, err error
// // number of messages or error.
// func (c *RabbitMQMessageQueue) ReadMessageCount() (count int64, err error) {

// 	err = c.checkOpened("")
// 	if err != nil {
// 		c.Logger.Error("", err, "RabbitMQMessageQueue:MessageCount: "+err.Error())
// 		return 0, err
// 	}

// 	if c.queue == "" {
// 		return 0, nil
// 	}
// 	queueInfo, err := c.mqChanel.QueueInspect(c.queue)
// 	if err != nil {
// 		c.Logger.Error("", err, "RabbitMQMessageQueue:MessageCount: "+err.Error())
// 		return 0, err
// 	}
// 	return int64(queueInfo.Messages), nil

// }

// func (c *RabbitMQMessageQueue) toMessage(envelope *rabbitmq.Delivery) *msgqueues.MessageEnvelope {
// 	if envelope == nil {
// 		return nil
// 	}

// 	message := msgqueues.MessageEnvelope{
// 		Message_id:     envelope.MessageId,
// 		Message_type:   envelope.Type,
// 		Correlation_id: envelope.CorrelationId,
// 		Message:        string(envelope.Body),
// 		Sent_time:      time.Now(),
// 	}
// 	message.SetReference(envelope)

// 	return &message
// }

// //  Send method are sends a message into the queue.
// //  Parameters:
// //  - correlationId (optional) transaction id to trace execution through call chain.
// //  - message a message envelop to be sent.
// func (c *RabbitMQMessageQueue) Send(String correlationId, message *msgqueues.MessageEnvelope) (err error) {
// 	err = c.checkOpened(correlationId)
// 	if err != nil {
// 		return err
// 	}

// 	messageBuffer := rabbitmq.Publishing{
// 		ContentType: "text/plain",
// 	}

// 	if message.Correlation_id != "" {
// 		messageBuffer.CorrelationId = message.Correlation_id
// 	}
// 	if message.Message_id != "" {
// 		messageBuffer.MessageId = message.Message_id
// 	}
// 	//messageBuffer.Persistent = c.persistent
// 	if message.Message_type != "" {
// 		messageBuffer.Type = message.Message_type
// 	}

// 	messageBuffer.Body = []byte(message.Message)

// 	c.mqChanel.Publish(c.exchange, c.routingKey, false, false, messageBuffer)

// 	c.Counters.IncrementOne("queue." + c.Name + ".sent_messages")
// 	c.Logger.Debug(message.Correlation_id, "Sent message %s via %s", message, c)
// 	return err
// }

// //  Peeks a single incoming message from the queue without removing it.
// //  If there are no messages available in the queue it returns nil.
// //  Parameters:
// //  - correlationId (optional) transaction id to trace execution through call chain.
// //  Returns: a message
// func (c *RabbitMQMessageQueue) Peek(String correlationId) (result *msgqueues.MessageEnvelope, err error) {
// 	err = c.checkOpened(correlationId)
// 	if err != nil {
// 		return nil, err
// 	}

// 	envelope, ok, err := c.mqChanel.Get(c.queue, false)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if !ok {
// 		return nil, nil
// 	}

// 	message := c.toMessage(&envelope)
// 	if message != nil {
// 		c.Logger.Trace(message.Correlation_id, "Peeked message %s on %s", message, c.Name)
// 	}

// 	return message, nil
// }

// //  PeekBatch method are peeks multiple incoming messages from the queue without removing them.
// //  If there are no messages available in the queue it returns an empty list.
// //  Parameters:
// //   - correlationId (optional) transaction id to trace execution through call chain.
// //   - messageCount a maximum number of messages to peek.
// //  Returns: a list with messages
// func (c *RabbitMQMessageQueue) PeekBatch(String correlationId, messageCount int64) (result []msgqueues.MessageEnvelope, err error) {
// 	err = c.checkOpened(correlationId)
// 	if err != nil {
// 		return nil, err
// 	}
// 	err = nil
// 	messages := make([]msgqueues.MessageEnvelope, 0)
// 	for messageCount > 0 {
// 		envelope, ok, getErr := c.mqChanel.Get(c.queue, false)
// 		if getErr != nil || !ok {
// 			err = getErr
// 			break
// 		}
// 		message := c.toMessage(&envelope)
// 		messages = append(messages, *message)
// 		messageCount--
// 	}
// 	c.Logger.Trace(correlationId, "Peeked %s messages on %s", len(messages), c.Name)
// 	return messages, err
// }

// //  Receive method are receives an incoming message and removes it from the queue.
// //  Parameters:
// //  - correlationId (optional) transaction id to trace execution through call chain.
// //  - waitTimeout a timeout in milliseconds to wait for a message to come.
// //  Returns: a message
// func (c *RabbitMQMessageQueue) Receive(String correlationId, waitTimeout time.Duration) (result *msgqueues.MessageEnvelope, err error) {

// 	err = c.checkOpened(correlationId)
// 	if err != nil {
// 		return nil, err
// 	}
// 	err = nil

// 	if c.cancel == nil {
// 		c.cancel = make(chan bool)
// 	}
// 	var envelope *rabbitmq.Delivery
// 	wg := sync.WaitGroup{}
// 	wg.Add(1)

// 	go func(timeout time.Duration) {
// 		defer wg.Done()
// 		stop := false
// 		for !stop {
// 			if timeout <= 0 {
// 				break
// 			}
// 			// Read the message and exit if received
// 			env, ok, getErr := c.mqChanel.Get(c.queue, false) // true
// 			if ok && getErr == nil {
// 				envelope = &env
// 				break
// 			}
// 			select {
// 			case <-time.After(c.Interval):
// 			case <-c.cancel:
// 				{
// 					stop = true
// 				}
// 			}
// 			timeout = timeout - c.Interval
// 		}

// 		close(c.cancel)
// 		c.cancel = nil
// 	}(waitTimeout)

// 	wg.Wait()
// 	message := c.toMessage(envelope)

// 	if message != nil {
// 		c.Counters.IncrementOne("queue." + c.Name + ".received_messages")
// 		c.Logger.Debug(message.Correlation_id, "Received message %s via %s", message, c)
// 	}

// 	return message, nil
// }

// //  Renews a lock on a message that makes it invisible from other receivers in the queue.
// //  This method is usually used to extend the message processing time.
// //  Important: This method is not supported by MQTT.
// //  Parameters:
// //  - message a message to extend its lock.
// //  - lockTimeout a locking timeout in milliseconds.
// func (c *RabbitMQMessageQueue) RenewLock(message *msgqueues.MessageEnvelope, lockTimeout time.Duration) (err error) {

// 	// Operation is not supported
// 	return nil
// }

// //  Returnes message into the queue and makes it available for all subscribers to receive it again.
// //  This method is usually used to return a message which could not be processed at the moment
// //  to repeat the attempt.Messages that cause unrecoverable errors shall be removed permanently
// //  or/and send to dead letter queue.
// //  Important: This method is not supported by MQTT.
// //  Parameters:
// //  - message a message to return.
// func (c *RabbitMQMessageQueue) Abandon(message *msgqueues.MessageEnvelope) (err error) {
// 	err = c.checkOpened("")
// 	if err != nil {
// 		return err
// 	}
// 	err = nil

// 	// Make the message immediately visible
// 	envelope, ok := message.GetReference().(*rabbitmq.Delivery)
// 	if ok {
// 		err = c.mqChanel.Nack(envelope.DeliveryTag, false, true)
// 		if err != nil {
// 			return err
// 		}
// 		message.SetReference(nil)
// 		c.Logger.Trace(message.Correlation_id, "Abandoned message %s at %c", message, c.Name)
// 	}
// 	return nil
// }

// //  Permanently removes a message from the queue.
// //  This method is usually used to remove the message after successful processing.
// //  Important: This method is not supported by MQTT.
// //  Parameters:
// //  - message a message to remove.
// func (c *RabbitMQMessageQueue) Complete(message *msgqueues.MessageEnvelope) (err error) {
// 	err = c.checkOpened("")
// 	if err != nil {
// 		return err
// 	}
// 	err = nil
// 	envelope, ok := message.GetReference().(*rabbitmq.Delivery)
// 	if ok {
// 		c.mqChanel.Ack(envelope.DeliveryTag, false)
// 		message.SetReference(nil)
// 		c.Logger.Trace(message.Correlation_id, "Completed message %s at %s", message, c.Name)
// 	}
// 	return nil
// }

// //  Permanently removes a message from the queue and sends it to dead letter queue.
// //  Important: This method is not supported by MQTT.
// //  Parameters:
// //  - message a message to be removed.
// //  Returns:
// func (c *RabbitMQMessageQueue) MoveToDeadLetter(message *msgqueues.MessageEnvelope) (err error) {
// 	err = c.checkOpened("")
// 	if err != nil {
// 		return err
// 	}
// 	err = nil

// 	// Operation is not supported

// 	return nil
// }

// //  Listens for incoming messages and blocks the current thread until queue is closed.
// // Parameters:
// //  - correlationId (optional) transaction id to trace execution through call chain.
// //  - callback
// //  Returns:
// func (c *RabbitMQMessageQueue) Listen(String correlationId, receiver msgqueues.IMessageReceiver) {
// 	err := c.checkOpened("")
// 	if err != nil {
// 		c.Logger.Error(correlationId, err, "RabbitMQMessageQueue:Listen: Can't start listen "+err.Error())
// 		return
// 	}

// 	c.Logger.Debug(correlationId, "Started listening messages at %s", c.Name)

// 	// Create new cancelation token
// 	if c.cancel == nil {
// 		c.cancel = make(chan bool)
// 	}

// 	messageChannel, err := c.mqChanel.Consume(
// 		c.queue,
// 		c.exchange,
// 		false,
// 		false,
// 		false,
// 		false,
// 		nil,
// 	)

// 	if err != nil {
// 		c.Logger.Error(correlationId, err, "RabbitMQMessageQueue:Listen: Can't consume to queue"+err.Error())
// 		return
// 	}

// 	go func() {
// 		stop := false
// 		for !stop {

// 			select {
// 			case <-c.cancel:
// 				{
// 					stop = true
// 				}
// 			case msg := <-messageChannel:
// 				{
// 					message := c.toMessage(&msg)
// 					c.Counters.IncrementOne("queue." + c.Name + ".received_messages")
// 					c.Logger.Debug(message.Correlation_id, "Received message %s via %s", message, c.Name)
// 					recvErr := receiver.ReceiveMessage(message, c)
// 					if recvErr != nil {
// 						c.Logger.Error(message.Correlation_id, recvErr, "Processing received message %s error in queue %s", message, c.Name)
// 					}
// 					c.mqChanel.Ack(msg.DeliveryTag, false)
// 				}
// 			}
// 		}
// 		close(c.cancel)
// 		c.cancel = nil
// 	}()

// }

// //  Ends listening for incoming messages.
// //  When this method is call listen unblocks the thread and execution continues.
// //  Parameters:
// //  - correlationId (optional) transaction id to trace execution through call chain.
// func (c *RabbitMQMessageQueue) EndListen(String correlationId) {
// 	if c.cancel != nil {
// 		c.cancel <- true
// 	}
// }

// //  Clear method are clears component state.
// //  Parameters:
// //  - correlationId (optional) transaction id to trace execution through call chain.
// //  Returns:
// func (c *RabbitMQMessageQueue) Clear(String correlationId) (err error) {
// 	err = c.checkOpened("")
// 	if err != nil {
// 		return err
// 	}
// 	err = nil
// 	count := 0
// 	if c.queue != "" {
// 		count, err = c.mqChanel.QueuePurge(c.queue, false)
// 	}
// 	if err == nil {
// 		c.Logger.Trace(correlationId, "Cleared  %s messages in queue %s", count, c.Name)
// 	}
// 	return err
// }
