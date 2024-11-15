<?php
namespace Packaged\Queue\Provider\Amqp;

use Exception;
use Packaged\Helpers\ValueAs;
use Packaged\Log\Log;
use Packaged\Queue\IBatchQueueProvider;
use Packaged\Queue\Provider\AbstractQueueProvider;
use Packaged\Queue\Provider\QueueConnectionException;
use Packaged\Queue\QueueException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Connection\Heartbeat\PCNTLHeartbeatSender;
use PhpAmqpLib\Exception\AMQPHeartbeatMissedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class AmqpQueueProvider extends AbstractQueueProvider
  implements IBatchQueueProvider
{
  // connection types
  const CONN_PUSH = 'push';
  const CONN_CONSUME = 'consume';
  const CONN_OTHER = 'other';

  protected $_hosts = [];
  protected $_hostsResetTime = null;
  protected $_hostsResetTimeMax = 300;
  protected $_hostsRetries = 3;
  protected $_hostsRetriesMax = 3;

  protected $_waitTime;

  /**
   * @var AMQPStreamConnection[]
   */
  protected $_connections = [];

  /**
   * @var AMQPChannel[]
   */
  protected $_channels = [];

  protected $_exchangeName;
  protected $_routingKey;
  protected $_exchange;

  protected $_persistentDefault = false;

  /**
   * How often to reconnect while consuming (in seconds)
   *
   * @var int
   */
  protected $_reconnectInterval = 1800;
  protected $_lastConnectTimes = [];

  /**
   * Saved QoS count for connection refresh
   *
   * @var null|int
   */
  protected $_qosCount = null;
  /**
   * Saved QoS size for connection refresh
   *
   * @var null|int
   */
  protected $_qosSize = null;

  /**
   * @var AMQPMessage[]
   */
  private static $_messageCache = [];

  private $_fixedConsumerCallback;
  protected $_consumerCallback;

  /**
   * If this is >0 then a message will be logged each time a push takes longer
   * than this number of milliseconds
   *
   * @var int
   */
  private $_slowPushThreshold = 0;

  /**
   * @var ?PCNTLHeartbeatSender
   */
  protected $_heartbeatSender;

  protected function _construct()
  {
    $this->_fixedConsumerCallback = [$this, 'consumerCallback'];
  }

  public function setSlowPushThreshold($threshold)
  {
    $this->_slowPushThreshold = $threshold;
    return $this;
  }

  /**
   * @param array $batch
   * @param null  $persistent
   *
   * @return $this
   * @throws QueueConnectionException
   */
  public function pushBatch(array $batch, $persistent = null)
  {
    $mandatory = $this->_getMandatoryFlag();
    $autoDeclare = $this->_getAutoDeclare();
    $publishConfirm = $this->_getPublishConfirm();
    // max no. of times to attempt declaring the queue
    $declareRetryLimit = 2;

    $needRetry = true;
    $needDeclare = false;
    $declareAttempts = 0;

    $returnCallback = null;
    if($mandatory)
    {
      $returnCallback = function ($replyCode, $replyText, $exchange, $routingKey)
      use (&$needRetry, &$needDeclare, &$autoDeclare, $declareAttempts, $declareRetryLimit) {
        if($autoDeclare && ($declareAttempts < $declareRetryLimit) && ($replyCode == 312))
        {
          $needDeclare = true;
          $needRetry = true;
        }
        else
        {
          throw new QueueConnectionException(
            'Error pushing message to exchange ' . $exchange
            . ' with routing key ' . $routingKey
            . ' : (' . $replyCode . ') ' . $replyText,
            $replyCode
          );
        }
      };
    }

    while($needRetry)
    {
      try
      {
        $needRetry = false;

        $this->_refreshConnection(self::CONN_PUSH);
        $ch = $this->_getChannel(self::CONN_PUSH);

        if($needDeclare)
        {
          Log::debug("Auto-declaring exchange and queue");
          $declareAttempts++;
          $this->declareExchange();
          $this->declareQueue();
          $this->bindQueue();
        }

        $exchangeName = $this->_getExchangeName();
        $routingKey = $this->_getRoutingKey();

        if($mandatory && $returnCallback)
        {
          $ch->set_return_listener($returnCallback);
        }

        foreach($batch as $data)
        {
          $ch->batch_basic_publish(
            $this->_getMessage($data, $persistent),
            $exchangeName,
            $routingKey,
            $mandatory
          );
        }

        $ch->publish_batch();

        if($publishConfirm || $mandatory)
        {
          try
          {
            $ch->wait_for_pending_acks_returns($this->_getPushTimeout());
          }
          catch(Exception $e)
          {
            $this->disconnect(self::CONN_PUSH);
            if($autoDeclare
              && ($declareAttempts < $declareRetryLimit)
              && ($e->getCode() == 404)
            )
            {
              $needRetry = true;
              $needDeclare = true;
            }
            else
            {
              throw $e;
            }
          }
        }
      }
      catch(AMQPHeartbeatMissedException $e)
      {
        $this->disconnect(self::CONN_PUSH);
        $needRetry = true;
      }
    }
    return $this;
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
        Log::warning('Slow push to queue. took ' . round($duration, 1) . 'ms');
      }
    }
    return $this;
  }

  public function consumerCallback(AMQPMessage $msg)
  {
    $callback = $this->_consumerCallback;
    $callback(
      json_decode($msg->body),
      $msg->getDeliveryTag()
    );
  }

  public function consume(callable $callback)
  {
    $this->_consumerCallback = $callback;
    $this->_refreshConnection(self::CONN_CONSUME);
    $channel = $this->_getChannel(self::CONN_CONSUME);
    $consumerId = $this->_getConsumerId();
    if(!isset($channel->callbacks[$consumerId]))
    {
      // register callback for this consumer
      $retry = true;
      $doneDeclare = false;
      while($retry)
      {
        $retry = false;
        try
        {
          $channel->basic_consume(
            $this->_getQueueName(),
            $consumerId,
            false,
            false,
            false,
            false,
            $this->_fixedConsumerCallback
          );
        }
        catch(AMQPHeartbeatMissedException $e)
        {
          $this->disconnect(self::CONN_CONSUME);
          $retry = true;
        }
        catch(AMQPProtocolChannelException $e)
        {
          if(($e->getCode() == 404)
            && $this->_getAutoDeclare() && (!$doneDeclare)
          )
          {
            // Attempt to auto-create the exchange and queue the first time we get a 404
            $this->_refreshConnection(self::CONN_CONSUME);
            $channel = $this->_getChannel(self::CONN_CONSUME);
            $this->declareExchange()->declareQueue()->bindQueue();
            $retry = true;
            $doneDeclare = true;
          }
          else
          {
            throw $e;
          }
        }
      }
    }
    else
    {
      // replace callback for this consumer
      $channel->callbacks[$consumerId] = $this->_fixedConsumerCallback;
    }

    // consumers bound, wait for message
    try
    {
      $channel->wait(null, false, $this->_getWaitTime());
    }
    catch(AMQPHeartbeatMissedException $e)
    {
      $this->disconnect(self::CONN_CONSUME);
      return false;
    }
    catch(AMQPTimeoutException $e)
    {
      return false;
    }
    return true;
  }

  protected function _processBatchMessage($msg, $tag = null)
  {
    $this->_batchData[$tag] = $msg;
  }

  protected function _getMessage($message, $persistent = null)
  {
    if($persistent === null)
    {
      $persistent = $this->_persistentDefault;
    }
    $persistent = $persistent ? 2 : 1;
    if(!isset(self::$_messageCache[$persistent]))
    {
      self::$_messageCache[$persistent] = new AMQPMessage(
        '',
        [
          'content_type'  => 'application/json',
          'delivery_mode' => $persistent,
        ]
      );
      self::$_messageCache[$persistent]->serialize_properties();
    }
    $msg = clone self::$_messageCache[$persistent];
    $msg->setBody(json_encode($message));
    return $msg;
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
    return ValueAs::bool($this->config()->getItem('mandatory', true));
  }

  protected function _getAutoDeclare()
  {
    return ValueAs::bool($this->config()->getItem('auto_declare', false));
  }

  protected function _getPublishConfirm()
  {
    return ValueAs::bool($this->config()->getItem('publish_confirm', false));
  }

  protected function _getPushTimeout()
  {
    return (float)$this->config()->getItem('push_timeout', 0);
  }

  protected function _getRoutingKey()
  {
    return $this->_routingKey;
  }

  protected function _getExchangeName()
  {
    return $this->_exchangeName;
  }

  public function purge()
  {
    $this->_getChannel(self::CONN_OTHER)->queue_purge($this->_getQueueName());
    return $this;
  }

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

  public function ack($deliveryTag)
  {
    try
    {
      $this->_getChannel(self::CONN_CONSUME)
        ->basic_ack($deliveryTag, false);
    }
    catch(AMQPHeartbeatMissedException $e)
    {
      $this->disconnect(self::CONN_CONSUME);
      $this->ack($deliveryTag);
    }
  }

  public function nack($deliveryTag, $requeueFailures = false)
  {
    try
    {
      $this->_getChannel(self::CONN_CONSUME)
        ->basic_reject($deliveryTag, $requeueFailures);
    }
    catch(AMQPHeartbeatMissedException $e)
    {
      $this->disconnect(self::CONN_CONSUME);
      $this->nack($deliveryTag, $requeueFailures);
    }
  }

  public function batchAck(array $tagResults, $requeueFailures = false)
  {
    if(count($tagResults) < 1)
    {
      return;
    }

    $channel = $this->_getChannel(self::CONN_CONSUME);
    $lastTag = null;
    // optimise ack/nack
    if(count(array_filter($tagResults)) >= (count($tagResults) / 2))
    {
      // more to ack than to nack, so reject individual ones and ack the rest
      foreach($tagResults as $tag => $passed)
      {
        if(!$passed)
        {
          $this->nack($tag, $requeueFailures);
        }
        else
        {
          $lastTag = $tag;
        }
      }
      if($lastTag)
      {
        $channel->basic_ack($lastTag, true);
      }
    }
    else
    {
      // more to nack than to ack, so ack individual ones and nack the rest
      foreach($tagResults as $tag => $passed)
      {
        if($passed)
        {
          $this->ack($tag);
        }
        else
        {
          $lastTag = $tag;
        }
      }
      if($lastTag)
      {
        $channel->basic_nack($lastTag, true, $requeueFailures);
      }
    }
  }

  /**
   * Reconnect periodically for safety
   * disconnect / reconnect after x time
   *
   * @param string $connectionMode
   */
  protected function _refreshConnection($connectionMode)
  {
    // check time of last connection
    $lastConnectTime = empty($this->_lastConnectTimes[$connectionMode])
      ? 0 : $this->_lastConnectTimes[$connectionMode];

    $channel = isset($this->_channels[$connectionMode])
      ? $this->_channels[$connectionMode] : null;

    // Disconnect if the channel exists but the connection is broken,
    // or if the connection has been open for too long
    if(($channel &&
        ((!$channel->getConnection())
          || (!$channel->getConnection()->isConnected())
        )
      )
      || (($lastConnectTime > 0)
        && ((time() - $lastConnectTime) >= $this->_reconnectInterval)
      )
    )
    {
      $this->disconnect($connectionMode);
    }
  }

  /**
   * @return array
   * @throws QueueConnectionException
   */
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
        throw new QueueConnectionException(
          'All hosts failed to connect ' . $this->_hostsRetriesMax .
          ' times within ' . $this->_hostsResetTimeMax . ' seconds'
        );
      }
    }
    shuffle($this->_hosts);
    return $this->_hosts;
  }

  /**
   * @param $connectionMode
   *
   * @return AMQPStreamConnection
   * @throws QueueConnectionException
   */
  protected function _getConnection($connectionMode)
  {
    while(empty($this->_connections[$connectionMode]))
    {
      $this->_getHosts();
      $host = reset($this->_hosts);
      $config = $this->config();
      try
      {
        $this->_connections[$connectionMode] = new AMQPStreamConnection(
          $host,
          $config->getItem('port', 5672),
          $config->getItem('username', 'guest'),
          $config->getItem('password', 'guest'),
          $config->getItem('vhost', '/'),
          false,
          'AMQPLAIN',
          null,
          'en_US',
          $config->getItem('connection_timeout', 3),
          $config->getItem('read_write_timeout', 3),
          null,
          ValueAs::bool($config->getItem('keepalive', false)),
          $config->getItem('heartbeat', 0)
        );
      }
      catch(Exception $e)
      {
        Log::error('AMQP host failed to connect [' . $e->getMessage() . '] (' . $host . ')');
        array_shift($this->_hosts);
      }
      $this->_persistentDefault = ValueAs::bool($config->getItem('persistent', false));
      $this->_lastConnectTimes[$connectionMode] = time();
    }

    try
    {
      if($this->_heartbeatSender)
      {
        $this->_heartbeatSender->unregister();
      }
      $this->_heartbeatSender = new PCNTLHeartbeatSender($this->_connections[$connectionMode]);
      $this->_heartbeatSender->register();
    }
    catch(AMQPRuntimeException $e)
    {
      Log::error('Unable to start heartbeat sender. ' . $e->getMessage());
    }

    return $this->_connections[$connectionMode];
  }

  /**
   * @param $connectionMode
   *
   * @return AMQPChannel
   * @throws QueueConnectionException
   * @throws Exception
   */
  protected function _getChannel($connectionMode)
  {
    $retries = 2;
    while(empty($this->_channels[$connectionMode]))
    {
      $connection = $this->_getConnection($connectionMode);
      try
      {
        $channel = $connection->channel();
        $this->_channels[$connectionMode] = $channel;
        $config = $this->config();

        switch($connectionMode)
        {
          case self::CONN_CONSUME:
            $qosSize = $this->_qosSize ?: $config->getItem('qos_size', 0);
            $qosCount = $this->_qosCount ?: $config->getItem('qos_count', 0);
            $this->_setPrefetch($channel, $qosCount, $qosSize);
            break;
          case self::CONN_PUSH:
            if($this->_getPublishConfirm())
            {
              $channel->confirm_select();
            }
            break;
        }
      }
      catch(Exception $e)
      {
        Log::error(
          'Error getting AMQP channel [' . $e->getMessage() . '] (' . $retries . ' retries remaining) '
        );
        $this->disconnect($connectionMode);
        if(!($retries--))
        {
          throw $e;
        }
      }
    }
    return $this->_channels[$connectionMode];
  }

  public function __destruct()
  {
    $this->disconnectAll();
  }

  public function disconnectAll()
  {
    // get list of connection modes that have been in use
    $modes = array_unique(
      array_merge(array_keys($this->_connections), array_keys($this->_channels))
    );
    foreach($modes as $mode)
    {
      $this->disconnect($mode);
    }
  }

  public function disconnect($connectionMode = null)
  {
    if($connectionMode)
    {
      $this->_disconnect($connectionMode);
    }
    else
    {
      $this->disconnectAll();
    }
  }

  private function _disconnect($connectionMode)
  {
    if((!empty($this->_channels[$connectionMode]))
      && ($this->_channels[$connectionMode] instanceof AMQPChannel)
    )
    {
      try
      {
        $this->_channels[$connectionMode]->wait_for_pending_acks_returns($this->_getPushTimeout());
      }
      catch(\Throwable $e)
      {
      }
      try
      {
        $this->_channels[$connectionMode]->basic_cancel($this->_getConsumerId());
      }
      catch(\Throwable $e)
      {
      }
      try
      {
        $this->_channels[$connectionMode]->close();
      }
      catch(\Throwable $e)
      {
      }
    }
    $this->_channels[$connectionMode] = null;

    if($this->_heartbeatSender)
    {
      try
      {
        $this->_heartbeatSender->unregister();
      }
      catch(\Throwable $e)
      {
      }
    }
    $this->_heartbeatSender = null;

    if((!empty($this->_connections[$connectionMode]))
      && ($this->_connections[$connectionMode] instanceof AbstractConnection)
    )
    {
      try
      {
        $this->_connections[$connectionMode]->close();
      }
      catch(\Throwable $e)
      {
      }
    }
    $this->_connections[$connectionMode] = null;
  }

  /**
   * @param callable $callback
   * @param          $batchSize
   *
   * @return bool
   * @throws QueueException
   */
  public function batchConsume(callable $callback, $batchSize)
  {
    if($this->_qosCount && $batchSize > $this->_qosCount)
    {
      throw new QueueException('Cannot consume batches greater than QoS');
    }
    return parent::batchConsume($callback, $batchSize);
  }

  public function setPrefetch($count, $size = 0)
  {
    return $this->_setPrefetch(
      $this->_getChannel(self::CONN_CONSUME),
      $count,
      $size
    );
  }

  protected function _setPrefetch(AMQPChannel $channel, $count, $size = 0)
  {
    $this->_qosCount = $count;
    $this->_qosSize = $size;
    $channel->basic_qos($size, $count, false);
    return $this;
  }

  public function getQosCount()
  {
    return $this->_qosCount;
  }

  public function getQueueInfo()
  {
    try
    {
      return $this->_getChannel(self::CONN_OTHER)
        ->queue_declare($this->_getQueueName(), true);
    }
    catch(AMQPProtocolChannelException $e)
    {
      // disconnect because the connection is now stale
      $this->disconnect(self::CONN_OTHER);
      if($e->amqp_reply_code !== 404)
      {
        throw $e;
      }
    }
    return false;
  }

  public function queueExists()
  {
    return $this->getQueueInfo() !== false;
  }

  public function declareQueue()
  {
    $config = $this->config();
    $this->_getChannel(self::CONN_OTHER)->queue_declare(
      $this->_getQueueName(),
      ValueAs::bool($config->getItem('queue_passive', false)),
      ValueAs::bool($config->getItem('queue_durable', true)),
      ValueAs::bool($config->getItem('queue_exclusive', false)),
      ValueAs::bool($config->getItem('queue_autodelete', false)),
      ValueAs::bool($config->getItem('queue_nowait', false)),
      new AMQPTable((array)$config->getItem('queue_args', null))
    );
    return $this;
  }

  public function deleteQueue()
  {
    $this->_getChannel(self::CONN_OTHER)->queue_delete($this->_getQueueName());
    return $this;
  }

  public function exchangeExists()
  {
    try
    {
      $this->_getChannel(self::CONN_OTHER)->exchange_declare(
        $this->_getExchangeName(),
        (string)$this->config()->getItem('exchange_type', 'direct'),
        true
      );
      return true;
    }
    catch(AMQPProtocolChannelException $e)
    {
      // disconnect because the connection is now stale
      $this->disconnect(self::CONN_OTHER);
      if($e->amqp_reply_code !== 404)
      {
        throw $e;
      }
    }
    return false;
  }

  public function declareExchange()
  {
    $config = $this->config();
    $this->_getChannel(self::CONN_OTHER)->exchange_declare(
      $this->_getExchangeName(),
      (string)$config->getItem('exchange_type', 'direct'),
      ValueAs::bool($config->getItem('exchange_passive', false)),
      ValueAs::bool($config->getItem('exchange_durable', true)),
      ValueAs::bool($config->getItem('exchange_autodelete', false)),
      ValueAs::bool($config->getItem('exchange_internal', false)),
      ValueAs::bool($config->getItem('exchange_nowait', false)),
      (array)$config->getItem('exchange_args', null)
    );
    return $this;
  }

  public function deleteExchange()
  {
    $this->_getChannel(self::CONN_OTHER)
      ->exchange_delete($this->_getExchangeName());
    return $this;
  }

  public function deleteQueueAndExchange()
  {
    $this->unbindQueue();
    $this->deleteQueue();
    $this->deleteExchange();
    return $this;
  }

  public function bindQueue()
  {
    $this->_getChannel(self::CONN_OTHER)->queue_bind(
      $this->_getQueueName(),
      $this->_getExchangeName(),
      $this->_getRoutingKey()
    );
    return $this;
  }

  public function unbindQueue()
  {
    $this->_getChannel(self::CONN_OTHER)->queue_unbind(
      $this->_getQueueName(),
      $this->_getExchangeName(),
      $this->_getRoutingKey()
    );
    return $this;
  }
}
