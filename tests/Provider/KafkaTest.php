<?php
namespace Packaged\Queue\Tests;

use Packaged\Config\ConfigSectionInterface;
use Packaged\Config\Provider\ConfigSection;
use Packaged\Queue\Provider\Amqp\AmqpQueueProvider;
use Packaged\Queue\Provider\Kafka\KafkaProvider;

class KafkaTest extends \PHPUnit_Framework_TestCase
{
  public function testAmqp()
  {
    $q = KafkaProvider::create('test')
      ->push('this is a test');

  }
}
