<?php
namespace Packaged\Queue\Tests;

use Packaged\Config\Provider\ConfigSection;
use Packaged\Queue\Provider\Amqp\AmqpModuleProvider;

class AmqpModuleTest extends \PHPUnit_Framework_TestCase
{
  private function _getQueue()
  {
    $conf = new ConfigSection();
    $conf->addItem('mandatory', true);
    $conf->addItem('publish_confirm', true);
    $conf->addItem('auto_declare', true);

    $q = AmqpModuleProvider::create('testqueue5');
    $q->configure($conf);
    return $q;
  }

  public function testPush()
  {
    $data = [];
    for($i = 0; $i < 5; $i++)
    {
      $data[] = ['testmsg' => $i];
    }
    $data2 = [];
    for($i = 0; $i < 5; $i++)
    {
      $data2[] = ['testmsg2' => $i];
    }

    $q = $this->_getQueue();
    $q->pushBatch($data);
    $q->pushBatch($data2);
  }

  public function testConsume()
  {
    $q = $this->_getQueue();

    $i = 0;
    $q->consume(
      function ($msg, $tag) use (&$i, $q)
      {
        echo "MESSASGE " . $i . ":\n";
        print_r($msg);
        echo "\n\n";
        $i++;

        $q->ack($tag);

        if($i >= 5)
        {
          return false;
        }
        return true;
      }
    );

    $q->consume(
      function ($msg, $tag) use (&$i, $q)
      {
        echo "MESSASGE " . $i . ":\n";
        print_r($msg);
        echo "\n\n";
        $i++;

        $q->ack($tag);
        return true;
      }
    );
  }
}
