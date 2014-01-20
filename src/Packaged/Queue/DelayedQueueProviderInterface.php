<?php
namespace Packaged\Queue;

interface DelayedQueueProviderInterface extends QueueProviderInterface
{
  public function delayedPush($data, $delay);
}
