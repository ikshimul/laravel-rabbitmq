<?php

namespace App\Services;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class RabbitMQService
{
    private $rabbitMQHost;

    private $rabbitMQPort;

    private $rabbitMQUser;

    private $rabbitMQPassword;

    private $rabbitMQVhost;


    public function __construct()
    {
        $this->rabbitMQHost = config('rabbitmq.rabbitmq_host');
        $this->rabbitMQPort = config('rabbitmq.rabbitmq_port');
        $this->rabbitMQUser = config('rabbitmq.rabbitmq_user');
        $this->rabbitMQPassword = config('rabbitmq.rabbitmq_password');
        $this->rabbitMQVhost = config('rabbitmq.rabbitmq_vhost');
    }

    public function createConnection(): array
    {
        $connection = new AMQPStreamConnection($this->rabbitMQHost, $this->rabbitMQPort, $this->rabbitMQUser, $this->rabbitMQPassword);

        $channel = $connection->channel();

        return [$connection, $channel];
    }

    public function shutDown($connection, $channel): void
    {
        $connection->close();

        $channel->close();
    }

    public function publishDirect($message, string $exchange = 'exchange_new', string $queue = 'order_queue', string $routing_key = 'order_created')
    {
        [$connection, $channel] = $this->createConnection();

        $channel->queue_declare($queue, false, true, false, false);
        $channel->exchange_declare($exchange, AMQPExchangeType::DIRECT, false, false, false);

        $channel->queue_bind($queue, $exchange, $routing_key);

        //$message = new AMQPMessage($message, array('content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT));

        $msg = new AMQPMessage(json_encode($message));
        $channel->basic_publish($msg, $exchange, $routing_key);

        $this->shutDown($connection, $channel);
    }

    public function consumeDirect(string $queue = 'order_queue')
    {
        [$connection, $channel] = $this->createConnection();

        $channel->queue_declare($queue, false, true, false, false);

        $callback = function ($msg): void {
            echo ' [x] Received ', json_decode($msg->body, true), "\n";
        };
        //$msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);

        $channel->basic_consume($queue, '', false, true, false, false, $callback);

        echo " [*] Waiting for the message. To exit press CTRL+C\n";

        while ($channel->is_consuming()) {
            $channel->wait();
        }

        $this->shutDown($connection, $channel);
    }

    public function publishTopic($message, string $routing_key = "order.created", string $exchange = "topic_exchange"): void
    {
        [$connection, $channel] = $this->createConnection();
        $channel->exchange_declare($exchange, AMQPExchangeType::TOPIC, false, true, false);

        $msg = new AMQPMessage(json_encode($message));
        $channel->basic_publish($msg, $exchange, $routing_key);
        $this->shutdown($connection, $channel);
    }

    public function consumeTopic(string $queue_name = "order_queue", string $topicPattern = "order.*", string $exchange = "topic_exchange"): void
    {
        [$connection, $channel] = $this->createConnection();
        $channel->exchange_declare($exchange, AMQPExchangeType::TOPIC, false, true, false);
        $channel->queue_declare($queue_name, false, true, false, false);
        $channel->queue_bind($queue_name, $exchange, $topicPattern);

        $callback = function ($msg) {
            echo ' [x] Received ', json_decode($msg->body, true), "\n";
        };

        $channel->basic_consume($queue_name, '', false, false, false, false, $callback);

        echo 'Waiting for new message on test_queue', " \n";
        while ($channel->is_consuming()) {
            $channel->wait();
        }
        $this->shutdown($connection, $channel);
    }

    public function publishFanout($message, string $exchange = "fanout_exchange"): void
    {
        [$connection, $channel] = $this->createConnection();
        $channel->exchange_declare($exchange, AMQPExchangeType::FANOUT, false, true, false);
        $msg = new AMQPMessage(json_encode($message));
        $channel->basic_publish($msg, $exchange, '');
        $this->shutdown($connection, $channel);
    }

    public function consumeFanout(string $exchange = "fanout_exchange", string $queue_name = "fanout_queue"): void
    {
        [$connection, $channel] = $this->createConnection();
        $channel->exchange_declare($exchange, AMQPExchangeType::FANOUT, false, true, false);
        $channel->queue_declare($queue_name, false, true, true, false);
        $channel->queue_bind($queue_name, $exchange, '');

        $callback = function ($msg) {
            echo ' [x] Received ', json_decode($msg->body, true), "\n";
        };

        $channel->basic_consume($queue_name, '', false, false, false, false, $callback);

        echo 'Waiting for new message on test_queue', " \n";
        while ($channel->is_consuming()) {
            $channel->wait();
        }
        $this->shutdown($connection, $channel);
    }

    public function publishHeaders(array $headers, $message, string $exchange = "header_exchange_3"): void
    {
        [$connection, $channel] = $this->createConnection();
        $channel->exchange_declare($exchange, AMQPExchangeType::HEADERS, false, true, false);
        $msg = new AMQPMessage(json_encode($message));
        $appHeaders = new AMQPTable(array_keys($headers));
        $msg->set('application_headers', $appHeaders);
        $channel->basic_publish($msg, $exchange, '');
        $this->shutdown($connection, $channel);
    }

    public function consumeHeaders(string $exchange = "header_exchange_3"): void
    {
        [$connection, $channel] = $this->createConnection();
        $channel->exchange_declare($exchange, AMQPExchangeType::HEADERS, false, true, false);
        list($queue_name, , ) = $channel->queue_declare('', false, false, true, false);

        $bindArguments = [
            "x-match" => "any",
            "notification-type-comment" => "comment",
            "notification-type-like" => "like",
        ];

        $channel->queue_bind($queue_name, $exchange, '', false, new AMQPTable($bindArguments));

        $callback = function (AMQPMessage $message) {
            echo PHP_EOL . ' [x] ', $message->getRoutingKey(), ':', $message->getBody(), "\n";
            echo 'Message headers follows' . PHP_EOL;
            var_dump($message->get('application_headers')->getNativeData());
            echo PHP_EOL;
        };

        $channel->basic_consume($queue_name, '', false, true, true, false, $callback);

        echo 'Waiting for new message on test_queue', " \n";
        while ($channel->is_consuming()) {
            try {
                $channel->wait(null, false, 2);
            } catch (AMQPTimeoutException $exception) {
            }
            echo '*' . PHP_EOL;
        }
        $this->shutdown($connection, $channel);
    }
}
