<?php
namespace Packaged\Queue;

interface IDelayedQueueProvider extends IQueueProvider
{
  public function delayedPush($data, $delay);
}
