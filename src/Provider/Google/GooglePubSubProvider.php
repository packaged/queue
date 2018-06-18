<?php
namespace Packaged\Queue\Provider\Google;

use Google\Cloud\Core\Exception\ConflictException;
use Google\Cloud\Core\Exception\NotFoundException;
use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Google\Cloud\PubSub\Topic;
use Packaged\Queue\IBatchQueueProvider;
use Packaged\Queue\Provider\AbstractQueueProvider;

class GooglePubSubProvider extends AbstractQueueProvider implements IBatchQueueProvider
{
  /** @var string */
  private $_topicName;
  /** @var string */
  private $_subscriptionName;
  /** @var PubSubClient */
  private $_client = null;
  /** @var Topic */
  private $_topic = null;
  /** @var Subscription */
  private $_subscription = null;
  /** @var Message[] */
  private $_unAckedMessages = [];

  public static function create($topicName, $subscriptionName = null)
  {
    if(!$subscriptionName)
    {
      $subscriptionName = $topicName;
    }
    /** @var static $o */
    $o = parent::create($topicName . '/' . $subscriptionName);
    $o->_topicName = $topicName;
    $o->_subscriptionName = $subscriptionName;
    return $o;
  }

  /**
   * @throws \Exception
   */
  public function getClient()
  {
    if($this->_client === null)
    {
      $options = [];
      $rawCreds = $this->config()->getItem('credentials', null);
      if($rawCreds)
      {
        $options['keyFile'] = $this->_loadCredentials($rawCreds);
      }
      $this->_client = new PubSubClient($options);
    }
    return $this->_client;
  }

  /**
   * @param string|array $credentials
   *
   * @return array
   * @throws \Exception
   */
  private function _loadCredentials($credentials)
  {
    // Load/decode credentials
    if(is_string($credentials))
    {
      if(file_exists($credentials))
      {
        if(!is_file($credentials))
        {
          throw new \Exception('The specified credentials file is not a file');
        }
        $credentials = file_get_contents($credentials);
      }

      $decoded = json_decode($credentials, true);
      if(!$decoded)
      {
        throw new \Exception('The provided credentials are not in valid JSON format');
      }
      $credentials = $decoded;
    }
    if((!is_array($credentials)) || (empty($credentials['project_id'])))
    {
      throw new \Exception(('Invalid credentials provided'));
    }
    return $credentials;
  }

  /**
   * @return Topic
   * @throws \Exception
   */
  protected function _getTopic()
  {
    if($this->_topic === null)
    {
      $this->_topic = $this->getClient()->topic($this->_topicName);
    }
    return $this->_topic;
  }

  /**
   * @return Subscription
   * @throws \Exception
   */
  protected function _getSubscription()
  {
    if($this->_subscription === null)
    {
      $this->_subscription = $this->getClient()->subscription($this->_subscriptionName, $this->_topicName);
    }
    return $this->_subscription;
  }

  /**
   * @throws \Exception
   */
  public function createTopic()
  {
    try
    {
      $this->_getTopic()->create();
    }
    catch(ConflictException $e)
    {
      if($e->getCode() != 409)
      {
        throw $e;
      }
    }
  }

  /**
   * @throws \Exception
   */
  public function createSubscription()
  {
    try
    {
      try
      {
        $this->_getSubscription()->create();
      }
      catch(NotFoundException $e)
      {
        $this->createTopic();
        $this->_getSubscription()->create();
      }
    }
    catch(ConflictException $e)
    {
      if($e->getCode() != 409)
      {
        throw $e;
      }
    }
  }

  /**
   * @param mixed $data
   *
   * @return string The message ID
   *
   * @throws \Exception
   */
  public function push($data)
  {
    $msgIds = $this->pushBatch([$data]);
    return reset($msgIds);
  }

  /**
   * @param array $batch
   *
   * @return string[] A list of message IDs indexed by the same keys as $batch
   * @throws NotFoundException
   * @throws \Exception
   */
  public function pushBatch(array $batch)
  {
    if(count($batch) < 1)
    {
      return null;
    }

    $messages = [];
    foreach($batch as $k => $data)
    {
      $messages[$k] = $this->_encodeMessage($data);
    }

    $topic = $this->_getTopic();
    try
    {
      return $topic->publishBatch($messages);
    }
    catch(NotFoundException $e)
    {
      if($this->_getAutoCreate() && ($e->getCode() == 404))
      {
        $this->_log('Auto-creating topic ' . $topic->name());
        $topic->create();
        return $topic->publishBatch($messages);
      }
      throw $e;
    }
  }

  /**
   * @param callable $callback
   *
   * @throws \Exception
   */
  public function consume(callable $callback)
  {
    $sub = $this->_getSubscription();
    if($this->_getAutoCreate() && (!$sub->exists()))
    {
      $this->createSubscription();
    }

    $messages = $sub->pull(['returnImmediately' => false, 'maxMessages' => 1]);
    if(count($messages) > 0)
    {
      $message = reset($messages);
      $data = $this->_decodeMessage($message->data());
      if($callback($data))
      {
        $sub->acknowledge($message);
      }
      else
      {
        $sub->modifyAckDeadline($message, 0);
      }
    }
  }

  /**
   * @param callable $callback
   * @param int      $batchSize
   *
   * @return bool True if there were messages to process, false if not
   * @throws \Exception
   */
  public function batchConsume(callable $callback, $batchSize)
  {
    $sub = $this->_getSubscription();
    if($this->_getAutoCreate() && (!$sub->exists()))
    {
      $this->createSubscription();
    }

    $toProcess = [];
    $messages = $sub->pull(['returnImmediately' => false, 'maxMessages' => $batchSize]);
    foreach($messages as $message)
    {
      $this->_unAckedMessages[$message->ackId()] = $message;
      $toProcess[$message->ackId()] = $this->_decodeMessage($message->data());
    }

    if(count($toProcess) > 0)
    {
      $callback($toProcess);
      return true;
    }
    return false;
  }

  /**
   * Ack a message that is being used in a batch consume
   *
   * @param string $ackId
   *
   * @throws \Exception
   */
  public function ack($ackId)
  {
    if(isset($this->_unAckedMessages[$ackId]))
    {
      $message = $this->_unAckedMessages[$ackId];
      $this->_getSubscription()->acknowledge($message);
      unset($this->_unAckedMessages[$ackId]);
    }
  }

  /**
   * Nack a message that is being used in a batch consume
   *
   * @param string $ackId
   *
   * @throws \Exception
   */
  public function nack($ackId)
  {
    if(isset($this->_unAckedMessages[$ackId]))
    {
      $message = $this->_unAckedMessages[$ackId];
      $this->_getSubscription()->modifyAckDeadline($message, 0);
      unset($this->_unAckedMessages[$ackId]);
    }
  }

  /**
   * Ack and Nack a batch of messages
   *
   * @param bool[] $results Array of ackId => bool
   *
   * @throws \Exception
   */
  public function batchAck(array $results)
  {
    /** @var Message[] $toAck */
    $toAck = [];
    /** @var Message[] $toNack */
    $toNack = [];
    foreach($results as $ackId => $shouldAck)
    {
      if(isset($this->_unAckedMessages[$ackId]))
      {
        if($shouldAck)
        {
          $toAck[] = $this->_unAckedMessages[$ackId];
        }
        else
        {
          $toNack[] = $this->_unAckedMessages[$ackId];
        }
      }
    }

    if(count($toAck) > 0)
    {
      $this->_getSubscription()->acknowledgeBatch($toAck);
      foreach($toAck as $msg)
      {
        unset($this->_unAckedMessages[$msg->ackId()]);
      }
    }

    if(count($toNack) > 0)
    {
      $this->_getSubscription()->modifyAckDeadlineBatch($toNack, 0);
      foreach($toNack as $msg)
      {
        unset($this->_unAckedMessages[$msg->ackId()]);
      }
    }
  }

  /**
   * @param mixed $message
   *
   * @return array Message array in the format required to pass to Topic::publish()
   */
  protected function _encodeMessage($message)
  {
    return ['data' => base64_encode(json_encode($message))];
  }

  /**
   * @param string $data
   *
   * @return array
   */
  protected function _decodeMessage($data)
  {
    return json_decode(base64_decode($data), true);
  }

  /**
   * @return bool
   * @throws \Exception
   */
  private function _getAutoCreate()
  {
    return (bool)$this->config()->getItem('auto_create', false);
  }
}
