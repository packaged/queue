<?php
namespace Packaged\Queue\Tests;

use Packaged\Queue\Provider\Amqp\AmqpQueueProvider;

class AmqpTest extends \PHPUnit_Framework_TestCase
{
  public function testAmqp()
  {
    $q = AmqpQueueProvider::create('test', 'testexchange')
      ->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('this is a test');

    $q->consume(
      function ($message, $deliveryTag) use ($q)
      {
        $this->assertEquals('this is a test', $message);
        $q->ack($deliveryTag);
      }
    );
  }

  public function testBatchAck()
  {
    $q = AmqpQueueProvider::create('test.batch.ack')
      ->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->purge();
    $q->config()->addItem('wait_time', 1);

    $total = 1000;

    $data = [];
    for($i = 0; $i < $total; $i++)
    {
      $data[] = 'message' . $i;
    }
    $q->pushBatch($data);

    $count = 0;
    while(true)
    {
      $c = 0;
      $q->batchConsume(
        function (array $messages) use ($q, &$c)
        {
          $results = [];
          foreach($messages as $tag => $message)
          {
            $c++;
            $results[$tag] = true;
          }
          $q->batchAck($results);
        },
        250
      );
      $count += $c;
      if(!$c)
      {
        break;
      }
    }
    $this->assertEquals($total, $count);
  }

  public function testBatchNack()
  {
    $q = AmqpQueueProvider::create('test.batch.nack')
      ->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->purge();
    $q->config()->addItem('wait_time', 1);

    $total = 1000;

    $data = [];
    for($i = 0; $i < $total; $i++)
    {
      $data[] = 'message' . $i;
    }
    $q->pushBatch($data);

    $count = 0;
    while(true)
    {
      $c = 0;
      $q->batchConsume(
        function (array $messages) use ($q, &$c)
        {
          $results = [];
          foreach($messages as $tag => $message)
          {
            $c++;
            $results[$tag] = false;
          }
          $q->batchAck($results);
        },
        250
      );
      $count += $c;
      if(!$c)
      {
        break;
      }
    }
    $this->assertEquals($total, $count);
  }

  public function testRequeue()
  {
    $q = AmqpQueueProvider::create('test.batch.requeue')
      ->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->purge();
    $q->config()->addItem('wait_time', 1);

    $total = 250;

    $data = [];
    for($i = 0; $i < $total; $i++)
    {
      $data[] = 'message' . $i;
    }
    $q->pushBatch($data);

    $q->batchConsume(
      function (array $messages) use ($q)
      {
        $results = [];
        foreach($messages as $tag => $message)
        {
          $results[$tag] = false;
        }
        $q->batchAck($results, true);
      },
      250
    );

    $count = 0;
    $q->batchConsume(
      function (array $messages) use ($q, &$count)
      {
        $results = [];
        foreach($messages as $tag => $message)
        {
          $count++;
          $results[$tag] = true;
        }
        $q->batchAck($results);
      },
      250
    );
    $this->assertEquals($total, $count);
  }
}
