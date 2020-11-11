<?php
namespace Packaged\Queue\Provider\Kafka;

use Kafka\Produce;
use Kafka\Protocol\Decoder;
use Kafka\Protocol\Encoder;
use Kafka\Socket;
use Packaged\Queue\IBatchQueueProvider;
use Packaged\Queue\Provider\AbstractQueueProvider;

class KafkaProvider extends AbstractQueueProvider
  implements IBatchQueueProvider
{
  /**
   * @var Socket
   */
  protected $_socket;

  /**
   * @return Produce
   */
  protected function _getSocket()
  {
    if($this->_socket === null)
    {
      $this->_socket = new Socket('localhost', '9092');
      $this->_socket->connect();
    }
    return $this->_socket;
  }

  public function pushBatch(array $batch)
  {
    $enc = new Encoder($this->_socket);
    $enc->produceRequest($data);

    $decoder = new Decoder($this->_socket);
    $result = $decoder->produceResponse();
    var_dump($result);
    return $this;
  }

  public function push($data)
  {
    $this->pushBatch([$data]);
    return $this;
  }

  public function consume(callable $callback)
  {
    $this->_getConsumer()->fetch();
  }
}
