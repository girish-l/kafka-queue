<?php

namespace Kafka;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;

class KafkaQueue extends Queue implements QueueContract
{
    protected $producer;
    protected $consumer;

    public function __construct($producer, $consumer)
    {
        $this->producer = $producer;
        $this->consumer = $consumer;
    }

    public function push($job, $data = '', $queue = null)
    {
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(1000);
    }

    public function pop($queue = null)
    {
        $this->consumer->subscribe([$queue]);

        try {
            $message = $this->consumer->consume(120 * 1000);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    echo 'arrived';
                    $job = unserialize($message->payload);
                    var_dump($job);
                    $job->handle();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo 'no more messages';
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        } catch (\Exception $e) {
            var_dump($e->getMessage());
        }
    }

    /**
     * @inheritDoc
     */
    public function size($queue = null)
    {
        // TODO: Implement size() method.
    }

    /**
     * @inheritDoc
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // TODO: Implement pushRaw() method.
    }

    /**
     * @inheritDoc
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        // TODO: Implement later() method.
    }
}
