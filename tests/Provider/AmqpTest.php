<?php
namespace Packaged\Queue\Tests;

use Packaged\Config\ConfigSectionInterface;
use Packaged\Config\Provider\ConfigSection;
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

  public function testQueueExists()
  {
    $q = AmqpQueueProvider::create('new_queue');
    $this->assertFalse($q->exists());
    $q->declareQueue();
    $this->assertTrue($q->exists());
    $q->deleteQueueAndExchange();
    $this->assertFalse($q->exists());
  }

  public function testBatchAck()
  {
    $config = new ConfigSection('', ['wait_time' => 1, 'qos_count' => 250]);
    $q = $this->_getQueue('test.batch.ack', $config)
      ->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->purge();

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
    $config = new ConfigSection('', ['wait_time' => 1]);
    $q = $this->_getQueue('test.batch.nack', $config)
      ->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->purge();

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
    $config = new ConfigSection('', ['wait_time' => 1]);
    $q = $this->_getQueue('test.batch.requeue', $config)
      ->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->purge();

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

  protected function _getQueue(
    $queueName, ConfigSectionInterface $config = null
  )
  {
    $q = AmqpQueueProvider::create($queueName);
    if($config)
    {
      $q->configure($config);
    }
    return $q;
  }
}
