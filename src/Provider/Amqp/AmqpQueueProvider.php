<?php
namespace Packaged\Queue\Provider\Amqp;

use Packaged\Queue\IBatchQueueProvider;
use Packaged\Queue\Provider\AbstractQueueProvider;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class AmqpQueueProvider extends AbstractQueueProvider
  implements IBatchQueueProvider
{
  protected $_hosts = [];
  protected $_hostsResetTime = null;
  protected $_hostsResetTimeMax = 300;
  protected $_hostsRetries = 3;
  protected $_hostsRetriesMax = 3;

  protected $_waitTime;

  /**
   * @var AbstractConnection
   */
  protected $_connection;

  /**
   * @var AMQPChannel
   */
  protected $_channel;

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
  protected $_lastConnectTime = 0;

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
   * This will be set to true the first time the consume() method is called.
   * It is used to prevent the connection from being refreshed in the push()
   * and pushBatch() methods.
   *
   * @var bool
   */
  private $_consumeMode = false;

  protected function _construct()
  {
    $this->_fixedConsumerCallback = [$this, 'consumerCallback'];
  }

  public function pushBatch(array $batch, $persistent = null)
  {
    if(!$this->_consumeMode)
    {
      $this->_refreshConnection();
    }
    $channel = $this->_getChannel();
    $i = 0;
    foreach($batch as $data)
    {
      $i++;
      $channel->batch_basic_publish(
        $this->_getMessage($data, $persistent),
        $this->_getExchangeName(),
        $this->_getRoutingKey()
      );
      if($i % 100 === 0)
      {
        $channel->publish_batch();
      }
    }
    $channel->publish_batch();
    return $this;
  }

  public function push($data, $persistent = null)
  {
    if(!$this->_consumeMode)
    {
      $this->_refreshConnection();
    }
    $msg = $this->_getMessage($data, $persistent);
    $this->_getChannel()->basic_publish(
      $msg,
      $this->_getExchangeName(),
      $this->_getRoutingKey()
    );
    return $this;
  }

  public function consumerCallback(AMQPMessage $msg)
  {
    $callback = $this->_consumerCallback;
    $callback(
      json_decode($msg->body),
      $msg->delivery_info['delivery_tag']
    );
  }

  public function consume(callable $callback)
  {
    $this->_consumeMode = true;
    $this->_consumerCallback = $callback;
    $this->_refreshConnection();
    $channel = $this->_getChannel();
    $consumerId = $this->_getConsumerId();
    if(!isset($channel->callbacks[$consumerId]))
    {
      // register callback for this consumer
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
    else
    {
      // replace callback for this consumer
      $channel->callbacks[$consumerId] = $this->_fixedConsumerCallback;
    }
    try
    {
      $channel->wait(null, true, $this->_getWaitTime());
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
      $this->_waitTime = $this->config()->getItem('wait_time', 30);
    }
    return $this->_waitTime;
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
    $this->_getChannel()->queue_purge($this->_getQueueName());
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

  public function deleteQueueAndExchange()
  {
    $this->deleteQueue();
    $this->deleteExchange();
  }

  public function ack($deliveryTag)
  {
    $this->_getChannel()->basic_ack($deliveryTag, false);
  }

  public function nack($deliveryTag, $requeueFailures = false)
  {
    $this->_getChannel()->basic_reject($deliveryTag, $requeueFailures);
  }

  public function batchAck(array $tagResults, $requeueFailures = false)
  {
    if(count($tagResults) < 1)
    {
      return;
    }

    $channel = $this->_getChannel();
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
   */
  protected function _refreshConnection()
  {
    // check time of last connection
    if((time() - $this->_lastConnectTime) >= $this->_reconnectInterval)
    {
      if($this->_connection)
      {
        $this->_log('Connection refresh');
      }
      $this->disconnect();
    }
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
   * @return AMQPStreamConnection
   * @throws \Exception
   */
  protected function _getConnection()
  {
    while($this->_connection === null)
    {
      $this->_getHosts();
      $host = reset($this->_hosts);
      $config = $this->config();
      try
      {
        $this->_connection = new AMQPStreamConnection(
          $host,
          $config->getItem('port', 5672),
          $config->getItem('username', 'guest'),
          $config->getItem('password', 'guest')
        );
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
      $this->_lastConnectTime = time();
    }
    return $this->_connection;
  }

  /**
   * @return AMQPChannel
   * @throws \Exception
   */
  protected function _getChannel()
  {
    $retries = 2;
    while($this->_channel === null)
    {
      $connection = $this->_getConnection();
      try
      {
        $this->_channel = $connection->channel();
        $config = $this->config();

        $qosSize = $this->_qosSize ?: $config->getItem('qos_size', 0);
        $qosCount = $this->_qosCount ?: $config->getItem('qos_count', 0);
        $this->setPrefetch($qosCount, $qosSize);
      }
      catch(\Exception $e)
      {
        $this->_log(
          'Error getting AMQP channel (' . $retries . ' retries remaining)'
        );
        $this->disconnect();
        if(!($retries--))
        {
          throw $e;
        }
      }
    }
    return $this->_channel;
  }

  public function __destruct()
  {
    $this->disconnect();
  }

  public function disconnect()
  {
    try
    {
      if($this->_channel !== null && $this->_channel instanceof AMQPChannel)
      {
        $this->_channel->close();
      }
    }
    catch(\Exception $e)
    {
    }
    $this->_channel = null;
    try
    {
      if($this->_connection !== null && $this->_connection instanceof AbstractConnection)
      {
        $this->_connection->close();
      }
    }
    catch(\Exception $e)
    {
    }
    $this->_connection = null;
    $this->_exchange = null;
  }

  public function batchConsume(callable $callback, $batchSize)
  {
    if($this->_qosCount && $batchSize > $this->_qosCount)
    {
      throw new \Exception('Cannot consume batches greater than QoS');
    }
    return parent::batchConsume($callback, $batchSize);
  }

  public function setPrefetch($count, $size = 0)
  {
    $this->_qosCount = $count;
    $this->_qosSize = $size;
    $this->_getChannel()->basic_qos($size, $count, false);
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
      return $this->_getChannel()->queue_declare($this->_getQueueName(), true);
    }
    catch(AMQPProtocolChannelException $e)
    {
      // disconnect because the connection is now stale
      $this->disconnect();
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
    $this->_getChannel()->queue_declare(
      $this->_getQueueName(),
      (bool)$config->getItem('queue_passive', false),
      (bool)$config->getItem('queue_durable', true),
      (bool)$config->getItem('queue_exclusive', false),
      (bool)$config->getItem('queue_autodelete', false),
      (bool)$config->getItem('queue_nowait', false),
      new AMQPTable((array)$config->getItem('queue_args', null))
    );
    return $this;
  }

  public function deleteQueue()
  {
    $this->_getChannel()->queue_delete($this->_getQueueName());
    return $this;
  }

  public function exchangeExists()
  {
    try
    {
      $this->_getChannel()->exchange_declare(
        $this->_getExchangeName(),
        (string)$this->config()->getItem('exchange_type', 'direct'),
        true
      );
      return true;
    }
    catch(AMQPProtocolChannelException $e)
    {
      // disconnect because the connection is now stale
      $this->disconnect();
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
    $this->_getChannel()->exchange_declare(
      $this->_getExchangeName(),
      (string)$config->getItem('exchange_type', 'direct'),
      (bool)$config->getItem('exchange_passive', false),
      (bool)$config->getItem('exchange_durable', true),
      (bool)$config->getItem('exchange_autodelete', false),
      (bool)$config->getItem('exchange_internal', false),
      (bool)$config->getItem('exchange_nowait', false),
      (array)$config->getItem('exchange_args', null)
    );
    return $this;
  }

  public function deleteExchange()
  {
    $this->_getChannel()->exchange_delete($this->_getExchangeName());
    return $this;
  }

  public function bindQueue()
  {
    $this->_getChannel()->queue_bind(
      $this->_getQueueName(),
      $this->_getExchangeName(),
      $this->_getRoutingKey()
    );
    return $this;
  }
}
