# Configuration Guide <br/>

Configuration structure follows the 
[standard configuration](https://github.com/pip-services/pip-services3-container-node/doc/Configuration.md) 
structure. 

### <a name="rabbitmq"></a> RabbitMQ

RabbitMQ service has the following configuration properties:
- name:                        name of the message queue

Example:
```yaml
- descriptor: "pip-services:messaging:rabbitmq:default:1.0"
  name: "test_queue"

```

For more information on this section read 
[Pip.Services Configuration Guide](https://github.com/pip-services/pip-services3-container-node/doc/Configuration.md#deps)