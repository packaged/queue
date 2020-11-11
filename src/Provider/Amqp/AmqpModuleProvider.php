<?php
namespace Packaged\Queue\Provider\Amqp;

use Packaged\Queue\IBatchQueueProvider;
use Packaged\Queue\Provider\AbstractQueueProvider;

class AmqpModuleProvider extends AbstractQueueProvider
  implements IBatchQueueProvider
{
  // connection types
  const CONN_PUSH = 'push';
  const CONN_CONSUME = 'consume';
  const CONN_OTHER = 'other';

  /** @var \AMQPConnection[] */
  //private static $_connectionCache = [];

  /** @var \AMQPConnection */
  protected $_connection;

  /** @var \AMQPChannel[] */
  private $_channels = [];

  private $_hosts = [];
  protected $_hostsResetTime = null;
  protected $_hostsResetTimeMax = 300;
  protected $_hostsRetries = 3;
  protected $_hostsRetriesMax = 3;

  protected $_persistentDefault = false;
  protected $_qosCount = null;
  protected $_qosSize = null;
  protected $_waitTime;
  protected $_exchangeName;
  protected $_routingKey;
  protected $_exchange;

  protected $_nextDeliveryTag = 0;
  protected $_pushedMessages = [];

  /**
   * If this is >0 then a message will be logged each time a push takes longer
   * than this number of milliseconds
   *
   * @var int
   */
  private $_slowPushThreshold = 0;

  public static function create(
    $queueName, $exchangeName = null, $routingKey = null
  )
  {
    /**
     * @var $object static
     */
    $object = parent::create($queueName);
    $object->_exchangeName = $exchangeName ?: $queueName;
    $object->_routingKey = $routingKey ?: $queueName;
    return $object;
  }

  public function setSlowPushThreshold($threshold)
  {
    $this->_slowPushThreshold = $threshold;
    return $this;
  }

  public function setPrefetch($count, $size)
  {
    $this->_qosCount = $count;
    $this->_qosSize = $size;
    if(!empty($this->_channels[self::CONN_CONSUME]))
    {
      $this->_channels[self::CONN_CONSUME]->qos($size, $count);
    }
  }

  protected function _getWaitTime()
  {
    if($this->_waitTime === null)
    {
      $this->_waitTime = (float)$this->config()->getItem('wait_time', 30);
    }
    return $this->_waitTime;
  }

  protected function _getMandatoryFlag()
  {
    return (bool)$this->config()->getItem('mandatory', true);
  }

  protected function _getAutoDeclare()
  {
    return (bool)$this->config()->getItem('auto_declare', false);
  }

  protected function _getPublishConfirm()
  {
    return (bool)$this->config()->getItem('publish_confirm', false);
  }

  protected function _getPushTimeout()
  {
    return (int)$this->config()->getItem('push_timeout', 0);
  }

  protected function _getRoutingKey()
  {
    return $this->_routingKey;
  }

  protected function _getExchangeName()
  {
    return $this->_exchangeName;
  }


  protected function _getHosts()
  {
    if(!$this->_hosts)
    {
      if((!$this->_hostsResetTime)
        || (time() - $this->_hostsResetTime > $this->_hostsResetTimeMax)
      )
      {
        $this->_hostsRetries = $this->_hostsRetriesMax;
        $this->_hostsResetTime = time();
      }
      if($this->_hostsRetries)
      {
        $this->_hosts = (array)$this->config()->getItem('hosts', 'localhost');
        $this->_hostsRetries--;
      }
      else
      {
        throw new \Exception(
          'All hosts failed to connect ' . $this->_hostsRetriesMax .
          ' times within ' . $this->_hostsResetTimeMax . ' seconds'
        );
      }
    }
    shuffle($this->_hosts);
    return $this->_hosts;
  }

  /**
   * @return \AMQPConnection
   * @throws \Exception
   */
  protected function _getConnection()
  {
    if(!extension_loaded('amqp'))
    {
      throw new \Exception('Required AMQP module not loaded.');
    }

    $config = $this->config();
    $port = $config->getItem('port', 5672);
    $username = $config->getItem('username', 'guest');

    /*
    // Make a connection cache key from hosts, port and username
    $cacheKey = md5(
      implode('|', [implode(',', $this->_getHosts()), $port, $username])
    );

    while(empty(self::$_connectionCache[$cacheKey])
      || (!self::$_connectionCache[$cacheKey]->isConnected())
    )*/
    while(empty($this->_connection) || (!$this->_connection->isConnected()))
    {
      $this->_getHosts();
      $host = reset($this->_hosts);
      try
      {
        $conn = new \AMQPConnection(
          [
            'host'     => $host,
            'port'     => $port,
            'login'    => $username,
            'password' => $config->getItem('password', 'guest'),
          ]
        );
        if($conn->pconnect())
        {
          // TODO: Configurable timeouts (although the amqplib version just uses the defaults of 3 seconds)
          $conn->setReadTimeout(3);
          $conn->setWriteTimeout(3);

          $this->_connection = $conn;
          //self::$_connectionCache[$cacheKey] = $conn;
        }
      }
      catch(\Exception $e)
      {
        $this->_log('AMQP host failed to connect (' . $host . ')');
        array_shift($this->_hosts);
      }
      $this->_persistentDefault = (bool)$config->getItem(
        'persistent',
        false
      );
    }
    //return self::$_connectionCache[$cacheKey];
    return $this->_connection;
  }

  public function disconnect()
  {
    if($this->_connection)
    {
      $this->_connection->pdisconnect();
    }
    $modes = array_unique(
      array_merge(array_keys($this->_connections), array_keys($this->_channels))
    );
    foreach($modes as $mode)
    {
      $this->_disconnectChannel($mode);
    }
  }

  protected function _getChannel($connectionMode)
  {
    if(empty($this->_channels[$connectionMode]))
    {
      $config = $this->config();
      $qosSize = $this->_qosSize ?: $config->getItem('qos_size', 0);
      $qosCount = $this->_qosCount ?: $config->getItem('qos_count', 0);

      $channel = new \AMQPChannel($this->_getConnection());
      if($connectionMode == self::CONN_CONSUME)
      {
        $channel->qos($qosSize, $qosCount);
      }
      $this->_channels[$connectionMode] = $channel;

      if($connectionMode == self::CONN_PUSH)
      {
        $this->_nextDeliveryTag = 1;
        $this->_pushedMessages = [];
      }
    }
    return $this->_channels[$connectionMode];
  }

  protected function _getQueue($channel)
  {
    $queue = new \AMQPQueue($channel);
    $queue->setName($this->_getQueueName());
    $queue->setFlags($this->_makeQueueFlags());
    $args = $this->config()->getItem('queue_args', null);
    if($args && is_array($args))
    {
      $queue->setArguments($args);
    }
    return $queue;
  }

  protected function _getExchange($channel)
  {
    $exchange = new \AMQPExchange($channel);
    $exchange->setName($this->_getExchangeName());
    $exchange->setFlags($this->_makeExchangeFlags());
    $exchange->setType(
      $this->config()->getItem("exchange_type", AMQP_EX_TYPE_DIRECT)
    );
    $args = $this->config()->getItem('exchange_args', null);
    if($args && is_array($args))
    {
      $exchange->setArguments($args);
    }
    return $exchange;
  }

  private function _makeFlags(array $items)
  {
    $flags = 0;
    foreach($items as [$configItem, $flagConst, $default])
    {
      if($this->config()->getItem($configItem, $default))
      {
        $flags = $flags | $flagConst;
      }
    }
    return $flags;
  }

  protected function _makeQueueFlags()
  {
    return $this->_makeFlags(
      [
        // [config_item, FLAG_CONST, default]
        ["queue_passive", AMQP_PASSIVE, false],
        ["queue_durable", AMQP_DURABLE, true],
        ["queue_exclusive", AMQP_EXCLUSIVE, false],
        ["queue_autodelete", AMQP_AUTODELETE, false],
        ["queue_nowait", AMQP_NOWAIT, false],
      ]
    );
  }

  protected function _makeExchangeFlags()
  {
    return $this->_makeFlags(
      [
        // [config_item, FLAG_CONST, default]
        ["exchange_passive", AMQP_PASSIVE, false],
        ["exchange_durable", AMQP_DURABLE, true],
        ["exchange_autodelete", AMQP_AUTODELETE, false],
        ["exchange_internal", AMQP_INTERNAL, false],
        ["exchange_nowait", AMQP_NOWAIT, false],
      ]
    );
  }

  protected function _disconnectChannel($connectionMode)
  {
    try
    {
      if((!empty($this->_channels[$connectionMode]))
        && ($this->_channels[$connectionMode] instanceof \AMQPChannel)
      )
      {
        $this->_channels[$connectionMode]->close();
      }
    }
    catch(\Exception $e)
    {
    }
    $this->_channels[$connectionMode] = null;
  }

  public function push($data, $persistent = null)
  {
    $startTime = microtime(true);
    $this->pushBatch([$data], $persistent);
    if($this->_slowPushThreshold > 0)
    {
      $duration = (microtime(true) - $startTime) * 1000;
      if($duration > $this->_slowPushThreshold)
      {
        error_log(
          'Slow push to queue ' . $this->_queueName . ' took '
          . round($duration, 1) . 'ms'
        );
      }
    }
    return $this;
  }

  public function pushBatch(array $batch, $persistent = null)
  {
    $mandatory = $this->_getMandatoryFlag();
    $autoDeclare = $this->_getAutoDeclare();
    $publishConfirm = $this->_getPublishConfirm();
    $ackTimeout = $this->_getPushTimeout();
    $maxPushAttempts = 2; // max no. of times to attempt pushing messages before failing

    if($persistent === null)
    {
      $persistent = $this->_persistentDefault;
    }

    $attributes = ['content_type' => 'application/json'];
    if($persistent)
    {
      $attributes['delivery_mode'] = AMQP_DURABLE;
    }

    $params = AMQP_NOPARAM;
    if($mandatory)
    {
      $params = $params | AMQP_MANDATORY;
    }

    $failedMessages = [];
    $needRetry = true;
    $needDeclare = false;

    $returnCallback = null;
    if($mandatory)
    {
      $returnCallback = function (
        $replyCode,
        $replyText,
        $exchange,
        $routingKey
      ) use (&$needRetry, &$needDeclare, $autoDeclare)
      {
        // The !$needDeclare here stops us trying to declare more than once
        if($autoDeclare && (!$needDeclare) && ($replyCode == 312))
        {
          $needDeclare = true;
          $needRetry = true;
        }
        else
        {
          throw new \Exception(
            'Error pushing message to exchange ' . $exchange
            . ' with routing key ' . $routingKey
            . ' : (' . $replyCode . ') ' . $replyText,
            $replyCode
          );
        }
        return false;
      };
    }

    $ackCallback = null;
    $nackCallback = null;
    if($publishConfirm)
    {
      $ackCallback = function ($deliveryTag, $multiple)
      {
        if($multiple)
        {
          $this->_getAndClearPushedMessagesUpToTag($deliveryTag);
        }
        else
        {
          unset($this->_pushedMessages[$deliveryTag]);
        }

        return count($this->_pushedMessages) > 0;
      };

      $nackCallback = function ($deliveryTag, $multiple)
      use (&$failedMessages)
      {
        if($multiple)
        {
          $messages = $this->_getAndClearPushedMessagesUpToTag($deliveryTag);
          $failedMessages = array_merge($failedMessages, $messages);
          unset($messages);
        }
        else if(isset($this->_pushedMessages[$deliveryTag]))
        {
          $failedMessages[] = $this->_pushedMessages[$deliveryTag];
          unset($this->_pushedMessages[$deliveryTag]);
        }
        return false;
      };
    }

    $timesAttempted = 0;
    while($needRetry && ($timesAttempted < $maxPushAttempts) && count($batch))
    {
      $needRetry = false;

      $channel = $this->_getChannel(self::CONN_PUSH);
      $channel->setReturnCallback($returnCallback);
      $channel->setConfirmCallback($ackCallback, $nackCallback);
      $exchange = $this->_getExchange($channel);

      if($publishConfirm)
      {
        $channel->confirmSelect();
      }

      if($needDeclare)
      {
        $this->_log("Auto-declaring exchange and queue");
        $exchange->declareExchange();
        $queue = $this->_getQueue($channel);
        $queue->declareQueue();
        $queue->bind($this->_getExchangeName(), $this->_getRoutingKey());
      }

      foreach($batch as $message)
      {
        $exchange->publish(
          json_encode($message),
          $this->_getRoutingKey(),
          $params,
          $attributes
        );

        if($publishConfirm)
        {
          $this->_pushedMessages[$this->_nextDeliveryTag] = $message;
          $this->_nextDeliveryTag++;
        }
      }

      try
      {
        if($publishConfirm)
        {
          // This also waits for basic.return
          $channel->waitForConfirm($ackTimeout);
        }
        else if($mandatory)
        {
          $channel->waitForBasicReturn($ackTimeout);
        }
      }
      catch(\AMQPException $e)
      {
        $this->_disconnectChannel(self::CONN_PUSH);

        // The !$needDeclare here stops us trying to declare more than once
        if($autoDeclare && (!$needDeclare) && ($e->getCode() == 404))
        {
          // Exchange does not exist, auto-create it if configured to do so
          $needDeclare = true;
          $needRetry = true;
        }
        else
        {
          throw $e;
        }
      }

      if(count($failedMessages) > 0)
      {
        $needRetry = true;
        $batch = $failedMessages;
      }
    }
    return $this;
  }

  private function _getAndClearPushedMessagesUpToTag($deliveryTag)
  {
    $messages = [];
    $allKeys = array_keys($this->_pushedMessages);
    foreach($allKeys as $k)
    {
      if($k <= $deliveryTag)
      {
        if(isset($this->_pushedMessages[$k]))
        {
          $messages[] = $this->_pushedMessages[$k];
          unset($this->_pushedMessages[$k]);
        }
      }
      else
      {
        break;
      }
    }
    return $messages;
  }

  public function consumerCallback(\AMQPEnvelope $msg)
  {
    $callback = $this->_consumerCallback;
    $callback(
      json_decode($msg->getBody()),
      $msg->getDeliveryTag()
    );
  }

  public function consume(callable $callback)
  {
    $retry = true;

    $queue = $this->_getQueue($this->_getChannel(self::CONN_CONSUME));

    while($retry)
    {
      $retry = false;

      $internalCallback = function (\AMQPEnvelope $msg, \AMQPQueue $queue)
      use ($callback)
      {
        $callback(json_decode($msg->getBody()), $msg->getDeliveryTag());
      };

      try
      {
        $consumerTag = $this->_getConsumerId();
        $queue->consume(
          $internalCallback,
          AMQP_NOPARAM,
          $consumerTag
        );
      }
      catch(\AMQPException $e)
      {
        // TODO: Auto-create queues and exchanges

        if($e->getCode() == 9) // AMQP_STATUS_SOCKET_ERROR
        {
          $this->_disconnect();
          $queue = $this->_getQueue($this->_getChannel(self::CONN_CONSUME));
          $retry = true;
        }
        else if($e->getCode() == 530) // Duplicate consumer ID
        {
          $queue->cancel($consumerTag);
          $retry = true;
        }
        else if($e->getMessage() == "Consumer timeout exceed")
        {
          // No more messages
          return false;
        }
        else
        {
          throw $e;
        }
      }
    }
    return true;
  }

  public function batchConsume(callable $callback, $batchSize)
  {
    if($this->_qosCount && $batchSize > $this->_qosCount)
    {
      throw new \Exception('Cannot consume batches greater than QoS');
    }
    return parent::batchConsume($callback, $batchSize);
  }

  public function ack($deliveryTag)
  {
    $this->_getQueue($this->_getChannel(self::CONN_CONSUME))
      ->ack($deliveryTag);
  }

  public function nack($deliveryTag, $requeueFailures = false)
  {
    $this->_getQueue($this->_getChannel(self::CONN_CONSUME))
      ->nack($deliveryTag);
  }
}
