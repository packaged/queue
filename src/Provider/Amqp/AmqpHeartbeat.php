<?php
namespace Packaged\Queue\Provider\Amqp;

use PhpAmqpLib\Connection\AbstractConnection;

/**
 * Sends heartbeats for every registered AMQP connection in the process from one SIGALRM handler.
 *
 * A process has a single SIGALRM handler, so one sender per connection would leave every
 * connection but the last registered without heartbeats.
 */
final class AmqpHeartbeat
{
  /** @var AbstractConnection[] keyed by spl_object_id */
  private static $_connections = [];
  private static $_interval = 0;

  public static function isSupported(): bool
  {
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
    self::$_connections[spl_object_id($connection)] = $connection;
    self::_arm();
  }

  public static function unregister(AbstractConnection $connection): void
  {
    if(!isset(self::$_connections[spl_object_id($connection)]))
    {
      return;
    }
    unset(self::$_connections[spl_object_id($connection)]);
    self::_arm();
  }

  public static function isRegistered(AbstractConnection $connection): bool
  {
    return isset(self::$_connections[spl_object_id($connection)]);
  }

  /**
   * Heartbeat each idle connection. Runs inside the SIGALRM handler.
   */
  public static function beat(): void
  {
    foreach(self::$_connections as $id => $connection)
    {
      if(!$connection->isConnected())
      {
        unset(self::$_connections[$id]);
        continue;
      }
      // A heartbeat frame written in the middle of another frame would corrupt the stream.
      if($connection->isWriting())
      {
        continue;
      }
      $interval = (int)ceil($connection->getHeartbeat() / 2);
      if(time() > $connection->getLastActivity() + $interval)
      {
        try
        {
          $connection->checkHeartBeat();
        }
        catch(\Throwable $e)
        {
          // Throwing here would unwind whatever code the signal interrupted.
          // The connection's next read or write raises the failure where it can be handled.
          unset(self::$_connections[$id]);
        }
      }
    }
    self::_arm();
  }

  private static function _arm(): void
  {
    $interval = 0;
    foreach(self::$_connections as $connection)
    {
      $connectionInterval = (int)ceil($connection->getHeartbeat() / 2);
      if($interval === 0 || $connectionInterval < $interval)
      {
        $interval = $connectionInterval;
      }
    }

    if($interval === 0)
    {
      if(self::$_interval !== 0)
      {
        pcntl_alarm(0);
        pcntl_signal(SIGALRM, SIG_IGN);
        self::$_interval = 0;
      }
      return;
    }

    if(self::$_interval === 0)
    {
      pcntl_async_signals(true);
      pcntl_signal(SIGALRM, [self::class, 'beat'], true);
    }
    self::$_interval = $interval;
    pcntl_alarm($interval);
  }
}
