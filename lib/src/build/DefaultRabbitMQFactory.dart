import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_components/pip_services3_components.dart';
import '../queues/RabbitMQMessageQueue.dart';
import '../queues/RabbitMQMessageQueueFactory.dart';

// Creates RabbitMQMessageQueue components by their descriptors.
// See RabbitMQMessageQueue
class DefaultRabbitMQFactory extends Factory {
  static final descriptor =
      Descriptor('pip-services', 'factory', 'rabbitmq', 'default', '1.0');
  static final Descriptor3 =
      Descriptor('pip-services3', 'factory', 'rabbitmq', 'default', '1.0');
  static final RabbitMQMessageQueueFactoryDescriptor =
      Descriptor('pip-services', 'factory', 'message-queue', 'rabbitmq', '1.0');
  static final RabbitMQMessageQueueFactory3Descriptor = Descriptor(
      'pip-services3', 'factory', 'message-queue', 'rabbitmq', '1.0');
  static final RabbitMQMessageQueueDescriptor =
      Descriptor('pip-services', 'message-queue', 'rabbitmq', '*', '1.0');
  static final RabbitMQMessageQueue3Descriptor =
      Descriptor('pip-services3', 'message-queue', 'rabbitmq', '*', '1.0');

// NewDefaultRabbitMQFactory method are create a new instance of the factory.
  DefaultRabbitMQFactory() : super() {
    registerAsType(
        RabbitMQMessageQueueFactoryDescriptor, RabbitMQMessageQueueFactory);
    registerAsType(
        RabbitMQMessageQueueFactory3Descriptor, RabbitMQMessageQueueFactory);
    registerAsType(RabbitMQMessageQueueDescriptor, RabbitMQMessageQueue);
    registerAsType(RabbitMQMessageQueue3Descriptor, RabbitMQMessageQueue);
  }
}
