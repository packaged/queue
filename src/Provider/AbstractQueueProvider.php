<?php
namespace Packaged\Queue\Provider;

use Packaged\Config\ConfigSectionInterface;
use Packaged\Config\ConfigurableInterface;
use Packaged\Config\Provider\ConfigSection;
use Packaged\Log\Log;
use Packaged\Queue\IQueueProvider;

abstract class AbstractQueueProvider
  implements IQueueProvider, ConfigurableInterface
{
  protected $_config;
  protected $_queueName;
  protected $_consumerId;

  protected $_batchData = [];

  /**
   * Configure the data connection
   *
   * @param ConfigSectionInterface $configuration
   *
   * @return static
   */
  public function configure(ConfigSectionInterface $configuration)
  {
    $this->_config = $configuration;
    return $this;
  }

  /**
   * @return ConfigSectionInterface
   */
  public function config()
  {
    if(!$this->_config)
    {
      $this->_config = new ConfigSection();
    }
    return $this->_config;
  }

  final protected function __construct()
  {
    $this->_construct();
  }

  protected function _construct()
  {
  }

  /**
   * @param $queueName
   *
   * @return static
   */
  public static function create($queueName)
  {
    $object = new static();
    $object->_queueName = $queueName;
    return $object;
  }

  protected function _getQueueName()
  {
    return $this->_queueName;
  }

  protected function _getConsumerId()
  {
    if($this->_consumerId === null)
    {
      $this->_consumerId =
        $this->_queueName . ':' . gethostname() . ':' . getmypid();
    }
    return $this->_consumerId;
  }

  public function batchConsume(callable $callback, $batchSize)
  {
    $this->_batchData = [];
    while(count($this->_batchData) < $batchSize)
    {
      if(!$this->consume([$this, '_processBatchMessage']))
      {
        break;
      }
    }
    if(count($this->_batchData) > 0)
    {
      $callback($this->_batchData);
      $this->_batchData = [];
      return true;
    }
    return false;
  }

  protected function _processBatchMessage($msg)
  {
    $this->_batchData[] = $msg;
  }

  protected function _log($message)
  {
    Log::debug('Queue (' . $this->_getQueueName() . '): ' . $message);
  }
}
