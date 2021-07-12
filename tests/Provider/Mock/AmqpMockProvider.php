<?php

namespace Packaged\Queue\Tests\Provider\Mock;

use Packaged\Queue\Provider\Amqp\AmqpQueueProvider;

class AmqpMockProvider extends AmqpQueueProvider
{
  protected $_disconnectCount = 0;
  protected $_unregisterHeartbeat = false;

  protected function _getConnection($connectionMode)
  {
    $conn = parent::_getConnection($connectionMode);
    if($this->_unregisterHeartbeat && $this->_heartbeatSender)
    {
      $this->_heartbeatSender->unregister();
    }
    return $conn;
  }

  public function unregisterHeartbeat()
  {
    $this->_unregisterHeartbeat = true;
    if($this->_unregisterHeartbeat && $this->_heartbeatSender)
    {
      $this->_heartbeatSender->unregister();
    }
    return $this;
  }

  public function getDisconnectCount()
  {
    return $this->_disconnectCount;
  }

  public function disconnect($connectionMode = null)
  {
    parent::disconnect($connectionMode);
    $this->_disconnectCount++;
  }
}
