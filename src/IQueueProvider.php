<?php
namespace Packaged\Queue;

interface IQueueProvider
{
  public function push($data);

  public function consume();
}
