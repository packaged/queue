<?php
namespace Packaged\Queue\Tests\Provider;

use Packaged\Config\ConfigSectionInterface;
use Packaged\Config\Provider\ConfigSection;
use Packaged\Queue\Provider\Amqp\AmqpQueueProvider;

class AmqpTest extends \PHPUnit_Framework_TestCase
{
  protected function _getProvider(string $queue, ?string $exchange = null)
  {
    $q = AmqpQueueProvider::create($queue, $exchange);
    $q->configure(new ConfigSection('', ['heartbeat' => 2]));
    return $q;
  }

  public function testAmqp()
  {
    $q = $this->_getProvider('test', 'testexchange');
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('this is a test');

    $q->consume(
      function ($message, $deliveryTag) use ($q) {
        $this->assertEquals('this is a test', $message);
        $q->ack($deliveryTag);
      }
    );
  }

  public function testQueueExists()
  {
    $q = $this->_getProvider('new_queue');
    $this->assertFalse($q->queueExists());
    $q->declareQueue();
    $this->assertTrue($q->queueExists());
    $q->deleteQueue();
    $this->assertFalse($q->queueExists());
  }

  public function testExchangeExists()
  {
    $q = $this->_getProvider('new_queue_e', 'new_exchange');
    $this->assertFalse($q->exchangeExists());
    $q->declareExchange();
    $this->assertTrue($q->exchangeExists());
    $q->deleteExchange();
    $this->assertFalse($q->exchangeExists());
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
        function (array $messages) use ($q, &$c) {
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
        function (array $messages) use ($q, &$c) {
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
      function (array $messages) use ($q) {
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
      function (array $messages) use ($q, &$count) {
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
    $q = $this->_getProvider($queueName);
    if($config)
    {
      $q->configure($config);
    }
    return $q;
  }

  /**
   * @param $config
   * @param $queueName
   * @param $createExchange
   * @param $createQueue
   * @param $createBinding
   *
   * @dataProvider mandatoryDataProvider
   */
  public function testMandatory(
    $config, $queueName, $createExchange, $createQueue, $createBinding
  )
  {
    $q = $this->_getProvider($queueName)->deleteQueueAndExchange();
    $q->configure(new ConfigSection('', $config));

    if($createExchange)
    {
      $q->declareExchange();
    }
    if($createQueue)
    {
      $q->declareQueue();
    }
    if($createBinding)
    {
      $q->bindQueue();
    }

    $q->push('test message ' . $queueName);

    $result = null;
    $q->consume(
      function ($message, $deliveryTag) use ($q, &$result) {
        $result = $message;
        $q->ack($deliveryTag);
      }
    );
    $this->assertEquals('test message ' . $queueName, $result);
  }

  public function mandatoryDataProvider()
  {
    $configs = [
      [
        'mandatory'       => true,
        'auto_declare'    => true,
        'publish_confirm' => true,
      ],
    ];

    $runs = [];
    foreach($configs as $config)
    {
      $runs = array_merge(
        $runs,
        [
          [$config, 'NoQueueOrExchange', false, false, false],
          [$config, 'ExchangeNoQueue', true, false, false],
          [$config, 'ExchangeQueueNoBinding', true, true, false],
          [$config, 'ExchangeQueueBinding', true, true, true],
        ]
      );
    }
    return $runs;
  }
}
