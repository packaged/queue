<?php
namespace Packaged\Queue;

interface IBatchQueueProvider extends IQueueProvider
{
  public function pushBatch(array $batch);
}
