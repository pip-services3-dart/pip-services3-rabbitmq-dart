import 'dart:io';
import 'package:test/test.dart';
import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_rabbitmq/pip_services3_rabbitmq.dart';
import './MessageQueueFixture.dart';

void main() {
  group('Test RabbitMQMessageQueue', () {
    RabbitMQMessageQueue queue;
    MessageQueueFixture fixture;

    setUpAll(() async {
      var rabbitmqHost = Platform.environment['RABBITMQ_HOST'] ?? 'localhost';
      var rabbitmqPort = Platform.environment['RABBITMQ_PORT'] ?? '5672';
      var rabbitmqExchange =
          Platform.environment['RABBITMQ_EXCHANGE'] ?? 'test';
      var rabbitmqQueue = Platform.environment['RABBITMQ_QUEUE'] ?? 'test';
      var rabbitmqUser = Platform.environment['RABBITMQ_USER'] ?? 'user';
      var rabbitmqPassword =
          Platform.environment['RABBITMQ_PASS'] ?? 'password';

      if (rabbitmqHost == null && rabbitmqPort == null) {
        return;
      }
      var queueConfig = ConfigParams.fromTuples([
        'exchange', rabbitmqExchange,
        'queue', rabbitmqQueue,
        'options.auto_create', true,
        //'connection.protocol', 'amqp',
        'connection.host', rabbitmqHost,
        'connection.port', rabbitmqPort,
        //'credential.username', rabbitmqUser,
        //'credential.password', rabbitmqPassword,
      ]);
      queue = RabbitMQMessageQueue('testQueue');
      queue.configure(queueConfig);
      fixture = MessageQueueFixture(queue);

      await queue.open('');
    });

    tearDownAll(() async {
      await queue.close('');
    });

    setUp(() async {
      await queue.clear('');
    });

    test('RabbitMQMessageQueue:Send Receive Message', () async {
      await fixture.testSendReceiveMessage();
    });

    test('RabbitMQMessageQueue:Receive Send Message', () async {
      await fixture.testReceiveSendMessage();
    });

    test('RabbitMQMessageQueue:Receive And Complete Message', () async {
      await fixture.testReceiveCompleteMessage();
    });

    test('RabbitMQMessageQueue:Receive And Abandon Message', () async {
      await fixture.testReceiveAbandonMessage();
    });

    test('RabbitMQMessageQueue:Send Peek Message', () async {
      await fixture.testSendPeekMessage();
    });

    test('RabbitMQMessageQueue:Peek No Message', () async {
      await fixture.testPeekNoMessage();
    });

    test('RabbitMQMessageQueue:On Message', () async {
      await fixture.testOnMessage();
    });
  });
}
