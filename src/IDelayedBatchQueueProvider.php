<?php
namespace Packaged\Queue;

interface IDelayedBatchQueueProvider
  extends IQueueProvider, IBatchQueueProvider, IDelayedQueueProvider
{
  public function delayedPushBatch(array $batch, $delay);
}
