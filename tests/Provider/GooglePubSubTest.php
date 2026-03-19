<?php
namespace Packaged\Queue\Tests\Provider;

use Packaged\Config\Provider\ConfigSection;
use Packaged\Queue\Provider\Google\GooglePubSubProvider;
use PHPUnit\Framework\TestCase;

class GooglePubSubTest extends TestCase
{
  protected function setUp(): void
  {
    parent::setUp();
    // Point the PubSub client at the local emulator
    putenv('PUBSUB_EMULATOR_HOST=localhost:8085');
  }

  protected function _getProvider(string $topic, string $subscription = null): GooglePubSubProvider
  {
    $q = GooglePubSubProvider::create($topic, $subscription);
    $q->configure(new ConfigSection('', ['auto_create' => true]));
    return $q;
  }

  protected function _uniqueName(string $base): string
  {
    return $base . '_' . uniqid();
  }

  public function testPushAndConsume()
  {
    $name = $this->_uniqueName('test_push_consume');
    $q = $this->_getProvider($name);

    $q->push('hello world');

    $result = null;
    $q->consume(function ($data) use (&$result) {
      $result = $data;
      return true; // ack
    });

    $this->assertEquals('hello world', $result);
  }

  public function testPushBatch()
  {
    $name = $this->_uniqueName('test_push_batch');
    $q = $this->_getProvider($name);

    $result = $q->pushBatch(['msg1', 'msg2', 'msg3']);
    $this->assertCount(3, $result['messageIds']);
  }

  public function testPushBatchEmpty()
  {
    $name = $this->_uniqueName('test_push_batch_empty');
    $q = $this->_getProvider($name);

    $result = $q->pushBatch([]);
    $this->assertNull($result);
  }

  public function testConsumeNack()
  {
    $name = $this->_uniqueName('test_nack');
    $q = $this->_getProvider($name);

    $q->push('nack me');

    // Nack the message (return false)
    $q->consume(function ($data) {
      return false;
    });

    // Message should be redelivered after nack
    $result = null;
    $q->consume(function ($data) use (&$result) {
      $result = $data;
      return true;
    });

    $this->assertEquals('nack me', $result);
  }

  public function testBatchConsume()
  {
    $name = $this->_uniqueName('test_batch_consume');
    $q = $this->_getProvider($name);

    $q->pushBatch(['b1', 'b2', 'b3']);

    $received = [];
    $hadMessages = $q->batchConsume(function ($messages) use ($q, &$received) {
      foreach($messages as $ackId => $data)
      {
        $received[] = $data;
        $q->ack($ackId);
      }
    }, 10);

    $this->assertTrue($hadMessages);
    $this->assertCount(3, $received);
  }

  public function testBatchAck()
  {
    $name = $this->_uniqueName('test_batch_ack');
    $q = $this->_getProvider($name);

    $q->pushBatch(['a1', 'a2', 'a3']);

    $received = [];
    $q->batchConsume(function ($messages) use ($q, &$received) {
      $results = [];
      foreach($messages as $ackId => $data)
      {
        $received[] = $data;
        $results[$ackId] = true;
      }
      $q->batchAck($results);
    }, 10);

    $this->assertCount(3, $received);
  }

  public function testBatchNack()
  {
    $name = $this->_uniqueName('test_batch_nack');
    $q = $this->_getProvider($name);

    $q->push('nack_batch');

    $q->batchConsume(function ($messages) use ($q) {
      $results = [];
      foreach($messages as $ackId => $data)
      {
        $results[$ackId] = false; // nack
      }
      $q->batchAck($results);
    }, 10);

    // Nacked message should be redelivered
    $redelivered = null;
    $q->batchConsume(function ($messages) use ($q, &$redelivered) {
      foreach($messages as $ackId => $data)
      {
        $redelivered = $data;
        $q->ack($ackId);
      }
    }, 10);

    $this->assertEquals('nack_batch', $redelivered);
  }

  public function testSingleAckAndNack()
  {
    $name = $this->_uniqueName('test_single_ack_nack');
    $q = $this->_getProvider($name);

    $q->pushBatch(['keep', 'reject']);

    $nackId = null;
    $q->batchConsume(function ($messages) use ($q, &$nackId) {
      foreach($messages as $ackId => $data)
      {
        if($data === 'keep')
        {
          $q->ack($ackId);
        }
        else
        {
          $q->nack($ackId);
          $nackId = $ackId;
        }
      }
    }, 10);

    // The nacked message should be redelivered
    $redelivered = null;
    $q->batchConsume(function ($messages) use ($q, &$redelivered) {
      foreach($messages as $ackId => $data)
      {
        $redelivered = $data;
        $q->ack($ackId);
      }
    }, 10);

    $this->assertEquals('reject', $redelivered);
  }

  public function testSeparateTopicAndSubscription()
  {
    $topic = $this->_uniqueName('test_topic');
    $sub = $this->_uniqueName('test_sub');
    $q = $this->_getProvider($topic, $sub);

    $q->push('separate names');

    $result = null;
    $q->consume(function ($data) use (&$result) {
      $result = $data;
      return true;
    });

    $this->assertEquals('separate names', $result);
  }

  public function testCreateDefaultsSubscriptionToTopic()
  {
    $name = $this->_uniqueName('test_default_sub');
    $q = GooglePubSubProvider::create($name);
    $q->configure(new ConfigSection('', ['auto_create' => true]));

    $q->push('default sub');

    $result = null;
    $q->consume(function ($data) use (&$result) {
      $result = $data;
      return true;
    });

    $this->assertEquals('default sub', $result);
  }

  public function testAckDeadlineConfig()
  {
    $name = $this->_uniqueName('test_ack_deadline');
    $q = GooglePubSubProvider::create($name);
    $q->configure(new ConfigSection('', ['auto_create' => true, 'ack_deadline' => 30]));

    $q->push('with deadline');

    $result = null;
    $q->consume(function ($data) use (&$result) {
      $result = $data;
      return true;
    });

    $this->assertEquals('with deadline', $result);
  }

  public function testMessageEncoding()
  {
    $name = $this->_uniqueName('test_encoding');
    $q = $this->_getProvider($name);

    $complex = ['key' => 'value', 'nested' => ['a' => 1]];
    $q->push($complex);

    $result = null;
    $q->consume(function ($data) use (&$result) {
      $result = $data;
      return true;
    });

    $this->assertEquals('value', $result->key);
    $this->assertEquals(1, $result->nested->a);
  }
}