<?php
namespace Packaged\Queue\Tests\Provider;

use Packaged\Config\ConfigSectionInterface;
use Packaged\Config\Provider\ConfigSection;
use Packaged\Queue\Tests\Provider\Mock\AmqpMockProvider;
use PhpAmqpLib\Exception\AMQPConnectionBlockedException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;

class AmqpTest extends TestCase
{
  protected function _getProvider(string $queue, ?string $exchange = null)
  {
    $q = AmqpMockProvider::create($queue, $exchange);
    $q->configure(new ConfigSection('', ['heartbeat' => 4, 'read_write_timeout' => 3]));
    $q->deleteQueueAndExchange();
    return $q;
  }

  public function testFailHeartbeat()
  {
    $q = $this->_getProvider('test_heartbeat')->unregisterHeartbeat();
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue();
    $q->push($q->config()->getItem('heartbeat'));
    self::assertEquals(0, $q->getDisconnectCount());
    $q->consume(
      function ($msg, $tag) use ($q) {
        self::_sleepFor((int)$msg * 3);

        $q->ack($tag);
        // expect one reconnect
        self::assertEquals(1, $q->getDisconnectCount());
      }
    );
  }

  public function testHeartbeat()
  {
    $q = $this->_getProvider('test_heartbeat_fn');
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue();

    // heartbeat on a specific connection should not disconnect
    $q->push('heartbeat_test');
    $checksBefore = $q->getHeartbeatCheckCount();
    $disconnectsBefore = $q->getDisconnectCount();
    $q->heartbeat(AmqpMockProvider::CONN_PUSH);
    self::assertGreaterThan($checksBefore, $q->getHeartbeatCheckCount());
    self::assertEquals($disconnectsBefore, $q->getDisconnectCount());

    // heartbeat on all connections should not disconnect
    $checksBefore = $q->getHeartbeatCheckCount();
    $q->heartbeat();
    self::assertGreaterThan($checksBefore, $q->getHeartbeatCheckCount());
    self::assertEquals($disconnectsBefore, $q->getDisconnectCount());
  }

  public function testHeartbeatAfterMissed()
  {
    $q = $this->_getProvider('test_heartbeat_missed')->unregisterHeartbeat();
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue();
    $q->push('trigger');

    // sleep past the heartbeat interval to cause a missed heartbeat
    $this->_idlePastHeartbeatTimeout($q);

    $checksBefore = $q->getHeartbeatCheckCount();
    $disconnectsBefore = $q->getDisconnectCount();
    $q->heartbeat(AmqpMockProvider::CONN_PUSH);
    // should have checked and disconnected due to missed heartbeat
    self::assertGreaterThan($checksBefore, $q->getHeartbeatCheckCount());
    self::assertGreaterThan($disconnectsBefore, $q->getDisconnectCount());
  }

  public function testHeartbeatKeepsEveryConnectionOfAProviderAlive()
  {
    $q = $this->_getProvider('test_heartbeat_all_modes');
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue();
    $q->push('open the push connection');
    $q->getQueueInfo();

    $this->_idlePastHeartbeatTimeout($q);

    $this->_assertConnectionOpen($q, AmqpMockProvider::CONN_PUSH);
    $this->_assertConnectionOpen($q, AmqpMockProvider::CONN_OTHER);
  }

  public function testHeartbeatKeepsConnectionsOfEveryProviderAlive()
  {
    $first = $this->_getProvider('test_heartbeat_first_provider');
    $first->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('open the first provider push connection');
    $second = $this->_getProvider('test_heartbeat_second_provider');
    $second->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('open the second provider push connection');

    $this->_idlePastHeartbeatTimeout($first);

    $this->_assertConnectionOpen($first, AmqpMockProvider::CONN_PUSH);
    $this->_assertConnectionOpen($second, AmqpMockProvider::CONN_PUSH);
  }

  public function testHeartbeatKeepsIdleConnectionAliveWhileOtherConnectionsChurn()
  {
    $idle = $this->_getProvider('test_heartbeat_idle_beside_churn');
    $idle->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('open the idle provider push connection');
    $churn = $this->_getProvider('test_heartbeat_churn');
    $churn->declareExchange()
      ->declareQueue()
      ->bindQueue();

    // Open and close a connection every second, more often than the heartbeat interval.
    $until = time() + (int)$idle->config()->getItem('heartbeat') * 3;
    while(time() < $until)
    {
      $churn->push('churn');
      $churn->disconnect();
      self::_sleepFor(1);
    }

    $this->_assertConnectionOpen($idle, AmqpMockProvider::CONN_PUSH);
  }

  public function testPushReconnectsAfterHeartbeatFailedDuringProcessPause()
  {
    $q = $this->_getProvider('test_heartbeat_after_pause');
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('before pause');

    // Hold SIGALRM through the pause, so the heartbeat handler first runs once the broker heartbeat is already missed.
    pcntl_sigprocmask(SIG_BLOCK, [SIGALRM]);
    try
    {
      $this->_idlePastHeartbeatTimeout($q);
    }
    finally
    {
      pcntl_sigprocmask(SIG_UNBLOCK, [SIGALRM]);
    }

    $disconnectsBefore = $q->getDisconnectCount();
    $q->push('after pause');
    self::assertGreaterThan($disconnectsBefore, $q->getDisconnectCount());
  }

  public function testDisconnectingOneModeKeepsTheOtherHeartbeating()
  {
    $q = $this->_getProvider('test_heartbeat_disconnect_one_mode');
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('open the push connection');
    $q->getQueueInfo();
    $q->disconnect(AmqpMockProvider::CONN_OTHER);

    $this->_idlePastHeartbeatTimeout($q);

    $this->_assertConnectionOpen($q, AmqpMockProvider::CONN_PUSH);
  }

  public function testConsumerCallbackPausedPastHeartbeatLeavesMessageForRedelivery()
  {
    $q = $this->_getProvider('test_heartbeat_paused_callback');
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('slow message');

    $consumed = $q->consume(
      function ($message, $deliveryTag) use ($q) {
        // Hold SIGALRM through the pause, so the heartbeat handler first runs once the broker heartbeat is missed.
        pcntl_sigprocmask(SIG_BLOCK, [SIGALRM]);
        try
        {
          $this->_idlePastHeartbeatTimeout($q);
        }
        finally
        {
          pcntl_sigprocmask(SIG_UNBLOCK, [SIGALRM]);
        }
        $q->ack($deliveryTag);
      }
    );
    self::assertFalse($consumed, 'consume() should report the batch as not consumed.');

    $redelivered = null;
    $q->consume(
      function ($message, $deliveryTag) use ($q, &$redelivered) {
        $redelivered = $message;
        $q->ack($deliveryTag);
      }
    );
    self::assertSame('slow message', $redelivered);
  }

  public function testBatchDropsDeliveriesFromAConsumeConnectionLostMidBatch()
  {
    $q = $this->_getProvider('test_heartbeat_batch_lost_connection');
    $q->configure(new ConfigSection('', ['heartbeat' => 4, 'read_write_timeout' => 3, 'wait_time' => 2]));
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('second');

    $q->afterNextBatchMessage = function () use ($q) {
      // Hold SIGALRM through the pause, so the heartbeat handler first runs once the broker heartbeat is missed.
      pcntl_sigprocmask(SIG_BLOCK, [SIGALRM]);
      try
      {
        $this->_idlePastHeartbeatTimeout($q);
      }
      finally
      {
        pcntl_sigprocmask(SIG_UNBLOCK, [SIGALRM]);
      }
    };

    $batch = [];
    $collect = function (array $messages) use (&$batch) {
      $batch = $messages;
    };
    $q->batchConsume($collect, 2);
    self::assertSame([], $batch, 'The batch held a delivery from the lost connection.');

    $q->batchConsume($collect, 2);
    self::assertSame(['second'], array_values($batch), 'The lost delivery should be redelivered.');
    $q->batchAck(array_fill_keys(array_keys($batch), true));
  }

  public function testReleasedConsumerProviderIsCollected()
  {
    $q = $this->_getProvider('test_heartbeat_released_consumer');
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('consume me');
    $q->consume(
      function ($message, $deliveryTag) use ($q) {
        $q->ack($deliveryTag);
      }
    );

    $released = \WeakReference::create($q);
    unset($q);
    // The first collection runs the cycle's destructors; the second frees it.
    gc_collect_cycles();
    gc_collect_cycles();

    self::assertNull($released->get(), 'The provider is still referenced after it was released.');
  }

  /**
   * @runInSeparateProcess
   * @preserveGlobalState disabled
   */
  public function testWithoutSignalsLeavesSigalrmAlone()
  {
    define('AMQP_WITHOUT_SIGNALS', true);
    $q = $this->_getProvider('test_heartbeat_without_signals');
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('open a connection');

    self::assertSame(SIG_DFL, pcntl_signal_get_handler(SIGALRM));
  }

  /**
   * Sleep with no AMQP traffic for long enough that the broker closes any connection that sent no heartbeats.
   */
  private function _idlePastHeartbeatTimeout(AmqpMockProvider $q)
  {
    self::_sleepFor((int)$q->config()->getItem('heartbeat') * 3);
  }

  /**
   * Sleep for the full duration. SIGALRM from the heartbeat handler interrupts usleep(), so sleep to a deadline.
   */
  private static function _sleepFor(float $seconds)
  {
    $deadline = microtime(true) + $seconds;
    while(($left = $deadline - microtime(true)) > 0)
    {
      usleep((int)ceil(min($left, 1) * 1000000));
    }
  }

  private function _assertConnectionOpen(AmqpMockProvider $q, string $connectionMode)
  {
    try
    {
      // Opening a channel is a round trip, so it fails if the broker has closed the connection.
      $q->getOpenConnection($connectionMode)->channel()->close();
    }
    catch(\Exception $e)
    {
      self::fail(sprintf(
        'The %s connection stopped heartbeating while idle: %s: %s',
        $connectionMode, get_class($e), $e->getMessage()
      ));
    }
    $this->addToAssertionCount(1);
  }

  public function testPushWithoutBlockedWaitFailsWhileBrokerBlocksPublishers()
  {
    $q = $this->_getBlockableProvider('test_blocked_no_wait', 0);
    try
    {
      $this->_blockPublishers($q);
      $this->expectException(AMQPConnectionBlockedException::class);
      $q->push('during alarm');
    }
    finally
    {
      $this->_unblockPublishers();
    }
  }

  public function testPushAfterBrokerUnblocksPublishersSucceedsWithoutBlockedWait()
  {
    $q = $this->_getBlockableProvider('test_blocked_then_unblocked', 0);
    try
    {
      $this->_blockPublishers($q);
      $this->_unblockPublishers();
      // connection.unblocked is now waiting on the socket, unread.
      sleep(1);
      $q->push('after alarm');
      self::assertFalse($q->getOpenConnection(AmqpMockProvider::CONN_PUSH)->isBlocked());
    }
    finally
    {
      $this->_unblockPublishers();
    }
  }

  public function testPushWaitsForBrokerToUnblockPublishers()
  {
    $q = $this->_getBlockableProvider('test_blocked_wait', 20);
    try
    {
      $this->_blockPublishers($q);
      $this->_unblockPublishersAfter(3);
      $start = microtime(true);
      $q->push('during alarm');
      self::assertGreaterThanOrEqual(2, microtime(true) - $start, 'The push did not wait for the broker to unblock.');
    }
    finally
    {
      $this->_unblockPublishers();
    }
  }

  public function testPushGivesUpWhenStillBlockedAfterBlockedWait()
  {
    $q = $this->_getBlockableProvider('test_blocked_wait_expires', 2);
    $start = microtime(true);
    try
    {
      $this->_blockPublishers($q);
      $start = microtime(true);
      $q->push('during alarm');
      self::fail('The push should fail once blocked_wait has passed.');
    }
    catch(AMQPConnectionBlockedException $e)
    {
      self::assertGreaterThanOrEqual(2, microtime(true) - $start, 'The push failed before blocked_wait passed.');
    }
    finally
    {
      $this->_unblockPublishers();
    }
  }

  public function testWaitWhileBlockedReturnsFalseWhenStillBlocked()
  {
    $q = $this->_getBlockableProvider('test_blocked_wait_method_expires', 0);
    try
    {
      $this->_blockPublishers($q);
      $start = microtime(true);
      self::assertFalse($q->waitWhileBlocked(AmqpMockProvider::CONN_PUSH, 2));
      self::assertGreaterThanOrEqual(2, microtime(true) - $start);
    }
    finally
    {
      $this->_unblockPublishers();
    }
  }

  public function testWaitWhileBlockedReturnsOnceBrokerUnblocksPublishers()
  {
    $q = $this->_getBlockableProvider('test_blocked_wait_method', 0);
    try
    {
      $this->_blockPublishers($q);
      $this->_unblockPublishersAfter(3);
      self::assertTrue($q->waitWhileBlocked(AmqpMockProvider::CONN_PUSH, 20));
      self::assertFalse($q->getOpenConnection(AmqpMockProvider::CONN_PUSH)->isBlocked());
    }
    finally
    {
      $this->_unblockPublishers();
    }
  }

  private function _getBlockableProvider(string $queue, float $blockedWait): AmqpMockProvider
  {
    if(!getenv('RABBIT_CONTAINER'))
    {
      self::markTestSkipped('Set RABBIT_CONTAINER to the RabbitMQ container, so the test can raise a memory alarm.');
    }
    $q = $this->_getProvider($queue);
    $q->configure(
      new ConfigSection(
        '', ['heartbeat' => 4, 'read_write_timeout' => 3, 'publish_confirm' => true, 'blocked_wait' => $blockedWait]
      )
    );
    $q->declareExchange()
      ->declareQueue()
      ->bindQueue()
      ->push('open the push connection');
    return $q;
  }

  /**
   * Raise a memory alarm, publish once so the broker blocks the push connection,
   * and read frames until the client has received connection.blocked.
   */
  private function _blockPublishers(AmqpMockProvider $q)
  {
    $this->_rabbitmqctl('set_vm_memory_high_watermark', '0.0000001');
    $connection = $q->getOpenConnection(AmqpMockProvider::CONN_PUSH);
    $connection->channel()->basic_publish(new AMQPMessage('trigger the block'), '', 'test_blocked_unrouted');
    $deadline = microtime(true) + 15;
    while(!$connection->isBlocked() && microtime(true) < $deadline)
    {
      try
      {
        $connection->wait(null, false, 1);
      }
      catch(AMQPTimeoutException $e)
      {
      }
    }
    self::assertTrue($connection->isBlocked(), 'The broker did not block the push connection.');
  }

  private function _unblockPublishers()
  {
    $this->_rabbitmqctl('set_vm_memory_high_watermark', '0.6');
  }

  private function _unblockPublishersAfter(int $seconds)
  {
    exec(sprintf(
      '(sleep %d; docker exec %s rabbitmqctl set_vm_memory_high_watermark 0.6) > /dev/null 2>&1 &',
      $seconds, escapeshellarg(getenv('RABBIT_CONTAINER'))
    ));
  }

  private function _rabbitmqctl(string ...$args)
  {
    exec(
      'docker exec ' . escapeshellarg(getenv('RABBIT_CONTAINER')) . ' rabbitmqctl '
      . implode(' ', array_map('escapeshellarg', $args)) . ' 2>&1',
      $output,
      $code
    );
    if($code !== 0)
    {
      self::fail('rabbitmqctl ' . implode(' ', $args) . ' failed: ' . implode("\n", $output));
    }
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
    $q = $this->_getProvider($queueName);
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
