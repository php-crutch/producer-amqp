# crutch/producer-amqp

AMQP protocol producer

# Install

```bash
composer require crutch/producer-amqp
```

```php
<?php

$producer = new Crutch\AmqpProducer\AmqpProducer('localhost', 5672, 'user', 'password');
$producer->produce('message 1', 'one');
```
