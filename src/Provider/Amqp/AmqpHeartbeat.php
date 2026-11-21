<?php
namespace Packaged\Queue\Provider\Amqp;

use PhpAmqpLib\Connection\AbstractConnection;

/**
 * Sends heartbeats for every registered AMQP connection in the process from one SIGALRM handler.
 *
 * A process has a single SIGALRM handler, so one sender per connection would leave every
 * connection but the last registered without heartbeats.
 *
 * Connections are held by weak reference, so the registry never keeps a provider alive.
 */
final class AmqpHeartbeat
{
  /** @var \WeakReference[] keyed by spl_object_id */
  private static $_connections = [];
  /** @var bool[] connections whose heartbeat failed, keyed by spl_object_id */
  private static $_failed = [];
  private static $_handlerInstalled = false;
  /** Unix time the pending alarm fires, or 0 when none is pending */
  private static $_nextBeat = 0;

  public static function isSupported(): bool
  {
    if(defined('AMQP_WITHOUT_SIGNALS') && AMQP_WITHOUT_SIGNALS)
    {
      return false;
    }
    return function_exists('pcntl_async_signals')
      && function_exists('pcntl_signal')
      && function_exists('pcntl_alarm')
      && PHP_OS_FAMILY !== 'Windows';
  }

  public static function register(AbstractConnection $connection): void
  {
    if(!self::isSupported() || $connection->getHeartbeat() < 1)
    {
      return;
    }
    $id = spl_object_id($connection);
    self::$_connections[$id] = \WeakReference::create($connection);
    unset(self::$_failed[$id]);
    self::_schedule();
  }

  public static function unregister(AbstractConnection $connection): void
  {
    $id = spl_object_id($connection);
    if(!self::_holds($id, $connection))
    {
      return;
    }
    unset(self::$_connections[$id], self::$_failed[$id]);
    if(!self::$_connections)
    {
      self::_cancel();
    }
  }

  /**
   * Whether a heartbeat on this connection failed in the signal handler.
   * The connection is unusable, so its owner should disconnect and reconnect.
   */
  public static function hasFailed(AbstractConnection $connection): bool
  {
    $id = spl_object_id($connection);
    return isset(self::$_failed[$id]) && self::_holds($id, $connection);
  }

  /**
   * Heartbeat each idle connection. Runs inside the SIGALRM handler.
   */
  public static function beat(): void
  {
    self::$_nextBeat = 0;
    foreach(self::$_connections as $id => $reference)
    {
      $connection = $reference->get();
      if($connection === null || !$connection->isConnected())
      {
        unset(self::$_connections[$id], self::$_failed[$id]);
        continue;
      }
      // A heartbeat frame written in the middle of another frame would corrupt the stream.
      if(isset(self::$_failed[$id]) || $connection->isWriting())
      {
        continue;
      }
      if(time() > $connection->getLastActivity() + self::_interval($connection))
      {
        try
        {
          $connection->checkHeartBeat();
        }
        catch(\Throwable $e)
        {
          // Throwing here would unwind whatever code the signal interrupted.
          // The owning provider sees the failure on its next push or consume and reconnects.
          self::$_failed[$id] = true;
        }
      }
    }
    self::_schedule();
  }

  private static function _holds(int $id, AbstractConnection $connection): bool
  {
    return isset(self::$_connections[$id]) && self::$_connections[$id]->get() === $connection;
  }

  private static function _interval(AbstractConnection $connection): int
  {
    return (int)ceil($connection->getHeartbeat() / 2);
  }

  /**
   * Arm the alarm for the shortest interval among live connections.
   * A pending alarm is only ever brought forward, so registry changes cannot postpone it.
   */
  private static function _schedule(): void
  {
    $interval = 0;
    foreach(self::$_connections as $id => $reference)
    {
      $connection = $reference->get();
      if($connection === null)
      {
        unset(self::$_connections[$id], self::$_failed[$id]);
        continue;
      }
      $connectionInterval = self::_interval($connection);
      if($interval === 0 || $connectionInterval < $interval)
      {
        $interval = $connectionInterval;
      }
    }

    if($interval === 0)
    {
      self::_cancel();
      return;
    }

    if(!self::$_handlerInstalled)
    {
      pcntl_async_signals(true);
      pcntl_signal(SIGALRM, [self::class, 'beat'], true);
      self::$_handlerInstalled = true;
    }

    $due = time() + $interval;
    if(self::$_nextBeat === 0 || $due < self::$_nextBeat)
    {
      pcntl_alarm($interval);
      self::$_nextBeat = $due;
    }
  }

  private static function _cancel(): void
  {
    if(self::$_handlerInstalled)
    {
      pcntl_alarm(0);
      pcntl_signal(SIGALRM, SIG_IGN);
      self::$_handlerInstalled = false;
    }
    self::$_nextBeat = 0;
  }
}
