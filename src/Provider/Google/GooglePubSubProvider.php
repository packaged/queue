<?php
namespace Packaged\Queue\Provider\Google;

use Packaged\Queue\IBatchQueueProvider;

class GooglePubSubProvider implements IBatchQueueProvider
{
  /** @var \Google_Client */
  private $_client;
  /** @var string */
  private $_projectId;
  /** @var \Google_Service_Pubsub|null */
  private $_pubSubService = null;
  /** @var \Google_Service_Pubsub_Topic */
  private $_topic;
  /** @var \Google_Service_Pubsub_Subscription|null */
  private $_subscription = null;

  private $_topicShortName;
  private $_subscriptionShortName;

  /**
   * @param \Google_Client $client
   * @param string         $projectId
   * @param string         $topicName
   * @param string|null    $subscriptionName
   * @param int|null       $ackDeadlineSeconds
   */
  public function __construct(
    \Google_Client $client, $projectId, $topicName, $subscriptionName = null,
    $ackDeadlineSeconds = null
  )
  {
    $this->_client = $client;
    $this->_projectId = $projectId;

    $this->_topicShortName = $topicName;
    $this->_subscriptionShortName = $subscriptionName;

    // For v1 API (currently on v1beta)
    //$topicPath = 'projects/' . $this->_project . '/topics/' . $this->_topic;
    $topicPath = '/topics/' . $projectId . '/' . $topicName;
    $this->_topic = new \Google_Service_Pubsub_Topic();
    $this->_topic->setName($topicPath);

    if($subscriptionName)
    {
      $this->_subscription = new \Google_Service_Pubsub_Subscription();
      $this->_subscription->setTopic($topicPath);
      $this->_subscription->setName(
        '/subscriptions/' . $projectId . '/' . $subscriptionName
      );
      if($ackDeadlineSeconds)
      {
        $this->_subscription->setAckDeadlineSeconds($ackDeadlineSeconds);
      }

      // for v1 API
      //$this->_subscription->setName(
      //  'projects/' . $this->_project . '/subscriptions/' . $this->_subscription
      //);
    }
  }

  protected function _log($msg)
  {
    error_log(
      'GooglePubSubProvider (' . $this->_topicShortName . ', '
      . $this->_subscriptionShortName . ') : ' . $msg
    );
  }

  /**
   * @return \Google_Client
   */
  protected function _getClient()
  {
    return $this->_client;
  }

  /**
   * @return string
   */
  public function getProjectId()
  {
    return $this->_projectId;
  }

  /**
   * @return \Google_Service_Pubsub
   */
  protected function _getPubSubService()
  {
    if($this->_pubSubService === null)
    {
      $this->_pubSubService = new \Google_Service_Pubsub($this->_getClient());
    }
    return $this->_pubSubService;
  }

  /**
   * @return \Google_Service_Pubsub_Topic
   */
  protected function _getTopic()
  {
    return $this->_topic;
  }

  /**
   * @return \Google_Service_Pubsub_Subscription|null
   */
  protected function _getSubscription()
  {
    return $this->_subscription;
  }

  /**
   * @param \Google_Service_Exception $e
   * @param string                    $resourceName
   *
   * @return bool
   */
  private function _isMissingResourceError(
    \Google_Service_Exception $e, $resourceName
  )
  {
    return ($e->getCode() == 404)
    && stristr(
      $e->getMessage(),
      'Resource not found (resource=' . basename($resourceName) . ')'
    );
  }

  /**
   * @param \Google_Service_Exception $e
   * @param string                    $resourceName
   *
   * @return bool
   */
  private function _isAlreadyExistsError(
    \Google_Service_Exception $e, $resourceName
  )
  {
    return ($e->getCode() == 409)
    && stristr(
      $e->getMessage(),
      'Resource already exists in the project (resource='
      . basename($resourceName) . ')'
    );
  }

  /**
   * @return \Google_Service_Pubsub_Topic
   * @throws \Google_Service_Exception
   */
  protected function _createTopic()
  {
    $this->_log('Creating topic ' . $this->_topicShortName);
    try
    {
      return $this->_getPubSubService()->topics->create($this->_getTopic());
    }
    catch(\Google_Service_Exception $e)
    {
      if($this->_isAlreadyExistsError($e, $this->_getTopic()->getName()))
      {
        return $this->_getTopic();
      }
      else
      {
        throw $e;
      }
    }
  }

  /**
   * Create the subscription in PubSub
   *
   * @return \Google_Service_Pubsub_Subscription
   * @throws \Exception
   */
  protected function _createSubscription()
  {
    $sub = $this->_getSubscription();
    if(!$sub)
    {
      throw new \Exception('Subscription not set');
    }

    $result = null;
    $retry = true;
    while($retry)
    {
      $this->_log('Creating subscription ' . $this->_subscriptionShortName);
      try
      {
        $retry = false;
        $result = $this->_getPubSubService()->subscriptions->create($sub);
      }
      catch(\Google_Service_Exception $e)
      {
        if($this->_isMissingResourceError(
          $e,
          $this->_getTopic()->getName()
        )
        )
        {
          $retry = true;
          $this->_createTopic();
        }
        else if($this->_isAlreadyExistsError($e, $this->_subscriptionShortName))
        {
          $retry = false;
          $result = $this->_getSubscription();
        }
        else
        {
          throw $e;
        }
      }
    }
    return $result;
  }

  /**
   * Make sure that the topic and subscription exist. It is a good idea to
   * call this before pushing anything into the queue.
   *
   * @throws \Exception
   * @throws \Google_Service_Exception
   */
  public function createComponents()
  {
    $this->_createTopic();
    $this->_createSubscription();
  }

  /**
   * @param array $batch
   *
   * @return string[] A list of message IDs indexed by the same keys as $batch
   * @throws \Google_Service_Exception
   */
  public function pushBatch(array $batch)
  {
    if(count($batch) < 1)
    {
      return null;
    }

    $toPush = [];
    $messageKeys = array_keys($batch);
    foreach($batch as $rawMsg)
    {
      $msg = new \Google_Service_Pubsub_PubsubMessage();
      $msg->setData(base64_encode(json_encode($rawMsg)));
      $toPush[] = $msg;
    }
    $topicPath = $this->_getTopic()->getName();
    $body = new \Google_Service_Pubsub_PublishBatchRequest();
    $body->setTopic($topicPath);
    $body->setMessages($toPush);

    $messageIds = [];
    $retry = true;
    while($retry)
    {
      try
      {
        $retry = false;
        $result = $this->_getPubSubService()->topics->publishBatch($body);
        $returnedIds = $result->getMessageIds();

        $i = 0;
        foreach($messageKeys as $key)
        {
          $messageIds[$key] = $returnedIds[$i];
          $i++;
        }
      }
      catch(\Google_Service_Exception $e)
      {
        if($this->_isMissingResourceError($e, $topicPath))
        {
          $retry = true;
          $this->_createTopic();
        }
        else
        {
          throw $e;
        }
      }
    }
    return $messageIds;
  }

  /**
   * @param $data
   *
   * @return \Google_Service_Pubsub_PublishBatchResponse|null
   * @throws \Google_Service_Exception
   */
  public function push($data)
  {
    return $this->pushBatch([$data]);
  }

  public function consume(callable $callback)
  {
    // TODO: Implement consume() method.
  }

  /**
   * @param callable $callback
   * @param int      $batchSize
   *
   * @throws \Exception
   * @throws \Google_Service_Exception
   */
  public function batchConsume(callable $callback, $batchSize)
  {
    if(!$this->_getSubscription())
    {
      throw new \Exception(
        'Cannot consume: No subscription has been configured'
      );
    }
    $subscriptionPath = $this->_getSubscription()->getName();
    $req = new \Google_Service_Pubsub_PullBatchRequest();
    $req->setSubscription($subscriptionPath);
    $req->setMaxEvents($batchSize);
    $req->setReturnImmediately(false);

    $retry = true;
    while($retry)
    {
      try
      {
        $retry = false;
        $toProcess = [];
        $result = $this->_getPubSubService()->subscriptions->pullBatch($req);
        $responses = $result->getPullResponses();
        foreach($responses as $response)
        {
          // also available in $response['pubsubEvent']['message']:
          //  $msg['messageId'];
          //  $msg['publishTime'];

          $toProcess[$response['ackId']] = json_decode(
            base64_decode(
              $response['pubsubEvent']['message']['data']
            )
          );

          $callback($toProcess);
        }
      }
      catch(\Google_Service_Exception $e)
      {
        if($this->_isMissingResourceError($e, $subscriptionPath))
        {
          $retry = true;
          $this->_createSubscription();
        }
        else
        {
          throw $e;
        }
      }
    }
  }

  public function ack($messageTag)
  {
    $this->batchAck([$messageTag => true]);
  }

  public function nack($messageTag, $requeue = true)
  {
    $this->batchAck([$messageTag => false], $requeue);
  }

  public function batchAck($results, $requeueFailures = true)
  {
    if(!$requeueFailures)
    {
      $results = array_filter($results);
    }
    $ackIds = array_keys($results);

    $body = new \Google_Service_Pubsub_AcknowledgeRequest();
    $body->setSubscription($this->_getSubscription()->getName());
    $body->setAckId($ackIds);
    $this->_getPubSubService()->subscriptions->acknowledge($body);
  }

  /**
   * @param string[] $ackIds
   * @param int      $deadline
   */
  public function modifyAckDeadline(array $ackIds, $deadline)
  {
    $body = new \Google_Service_Pubsub_ModifyAckDeadlineRequest();
    $body->setSubscription($this->_getSubscription()->getName());
    $body->setAckIds($ackIds);
    $body->setAckDeadlineSeconds($deadline);
    $this->_getPubSubService()->subscriptions->modifyAckDeadline($body);
  }
}
