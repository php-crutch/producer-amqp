<?php

declare(strict_types=1);

namespace Crutch\AmqpProducer;

use Crutch\Producer\Producer;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use RuntimeException;
use Throwable;

final class AmqpProducer implements Producer
{
    /** @var array{0:string,1:null|int,2:string,3:string} */
    private array $settings;
    private ?AMQPStreamConnection $connection = null;
    private ?AMQPChannel $channel = null;

    /**
     * @param string $host
     * @param int $port
     * @param string $user
     * @param string $password
     */
    public function __construct(
        string $host,
        int $port = 5672,
        string $user = 'guest',
        string $password = 'guest'
    ) {
        $this->settings = [$host, $port, $user, $password];
    }

    public function produce(string $message, string $topic, float $delay = 0): void
    {
        $delay = max(0.0, $delay);
        if ($delay === 0.0) {
            $this->send($message, $topic);
            return;
        }
        $this->defer($message, $topic, $delay);
    }

    private function send(string $message, string $topic): void
    {
        $this->declareTopic($topic);

        $channel = $this->getChannel();
        $amqpMessage = new AMQPMessage($message);
        $channel->basic_publish($amqpMessage, $topic);
    }

    private function defer(string $payload, string $topic, float $seconds): void
    {
        $delay = (int)($seconds * 1000);
        if ($delay <= 0) {
            $this->produce($payload, $topic);
            return;
        }

        $delayedTopic = $this->declareDelayedTopic($topic, $delay);

        $channel = $this->getChannel();
        $message = new AMQPMessage($payload, ['delivery_mode' => 2]);
        $channel->basic_publish($message, $delayedTopic);
    }

    /**
     * @param string $topic
     */
    private function declareTopic(string $topic): void
    {
        $channel = $this->getChannel();

        $channel->exchange_declare($topic, 'fanout', false, true, false);
    }

    private function declareDelayedTopic(string $topic, int $delay): string
    {
        $channel = $this->getChannel();

        $suffix = '.delayed-' . $delay . '-ms';

        $delayedTopic = $topic . $suffix;
        $delayedQueue = $topic . $suffix;
        $channel->queue_declare($delayedQueue, false, true, false, true, false, [
            'x-message-ttl' => ['I', $delay],
            'x-expires' => ['I', $delay + 1000],
            'x-dead-letter-exchange' => ['S', $topic]
        ]);

        $channel->exchange_declare($delayedTopic, 'direct', false, true, false);
        $channel->queue_bind($delayedQueue, $delayedTopic);
        return $delayedTopic;
    }

    private function getChannel(): AMQPChannel
    {
        if (is_null($this->channel)) {
            $this->channel = $this->getConnection()->channel();
        }
        return $this->channel;
    }

    private function getConnection(): AMQPStreamConnection
    {
        if (is_null($this->connection)) {
            $this->connection = $this->createConnection();
        }
        if (!$this->connection->isConnected()) {
            try {
                $this->connection->reconnect();
            } catch (Throwable $exception) {
                throw new RuntimeException('Can not reconnect', 0, $exception);
            }
        }
        return $this->connection;
    }

    private function createConnection(): AMQPStreamConnection
    {
        [$host, $port, $user, $password] = $this->settings;
        $attempt = 1;
        $connection = null;
        while ($attempt < 10) {
            try {
                $connection = new AMQPStreamConnection($host, $port, $user, $password);
            } catch (Throwable $exception) {
            }
            $attempt++;
            usleep(100);
        }
        if (!is_null($connection)) {
            return $connection;
        }
        throw new RuntimeException('Can not create connection');
    }

    public function __destruct()
    {
        if (!is_null($this->channel)) {
            try {
                $this->channel->close();
            } catch (Throwable $exception) {
            }
        }
        if (!is_null($this->connection)) {
            try {
                $this->connection->close();
            } catch (Throwable $exception) {
            }
        }
    }
}
