<?php
namespace Packaged\Queue;

interface QueueProviderInterface
{
  public function push($data);

  public function consume();
}
