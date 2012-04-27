<?
/* MessageQueue.class.php - Class that implements System V message queue, ideal for IPC.
 * Copyright (C) 2007 Erik Osterman <e@osterman.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/* File Authors:
 *   Erik Osterman <e@osterman.com>
 */

if( !defined('IPC_CREAT') )
  define('IPC_CREAT', 01000);     // Create key if key does not exist.

if( !defined('IPC_EXCL') )
  define('IPC_EXCL', 02000);      // Fail if key exists. 

if( !defined('IPC_NOWAIT') )
  define('IPC_NOWAIT', 04000);    // Return error on wait


class MessageQueue
{
  protected $key;
  protected $queue;
  protected $blocking;
  protected $serialize;
  protected $messageType;
  
  public function __construct( $key, $blocking = true, $serialize = true )
  {
    $this->attach( $key );
    $this->setBlocking($blocking);
    $this->setSerialize($serialize);
    $this->setMessageType(1);
  }

  public function __destruct()
  {
//    $this->detach();
    unset($this->key);
    unset($this->queue);
    unset($this->blocking);
    unset($this->serialize);
    unset($this->messageType);
  }

  public function __key()
  {
    return $this->key;
  }

  public function __get($property)
  {
    switch( $property )
    {
      case 'select':
      case 'size':
      case 'qnum':
        return (int)$this->stats( 'msg_qnum');
      case 'pop':
        return $this->pop();
      case 'blocking':
        return $this->blocking;
      case 'key':
        return $this->key;
      case 'uid':
        return $this->stats( 'msg_perm.uid');
      case 'gid':
        return $this->stats( 'msg_perm.gid' );
      case 'mode':
        return $this->stats( 'msg_perm.mode' );
      case 'stime':
        return $this->stats( 'msg_stime') ;
      case 'rtime':
        return $this->stats( 'msg_rtime') ;
      case 'ctime':
        return $this->stats( 'msg_ctime' );
      case 'bytes':
      case 'qbytes':
        return (int)$this->stats( 'msg_qbytes' );
      case 'lspid':
        return $this->stats( 'msg_lspid' );
      case 'lrpid':
        return $this->stats( 'msg_lrpid' );
      default:
        throw new Exception( get_class($this) . "::$property not handled");
    }
  }

  public function __set($property, $value)
  {
    switch( $property )
    {
      case 'push':
        return $this->push($value);
      case 'blocking':
        return $this->blocking = $value;
      case 'uid':
        return msg_set_queue( $this->queue, Array( 'msg_perm.uid' => $value ) );
      case 'gid':
        return msg_set_queue( $this->queue, Array( 'msg_perm.gid' => $value ) );
      case 'mode':
        return msg_set_queue( $this->queue, Array( 'msg_perm.mode' => $value ) );
      default:
        throw new Exception( get_class($this) . "::$property cannot be set");
    }
  }

  public function __unset($property)
  {
    throw new Exception( get_class($this) . "::$property cannot be unset");
  }

  public function reattach()
  {
    $this->attach( $this->key );
  }

  public function setBlocking($blocking)
  {
    return $this->blocking = $blocking;
  }

  public function setSerialize($serialize)
  {
    return $this->serialize = $serialize;
  }

  public function setMessageType($type)
  {
    return $this->messageType = $type;
  }


  // FIXME need a static class that decodes standard errnos
  public function errno( $errno )
  {
    switch( $errno )
    {
      default:
        return "errno=$errno " . " errstr=" . posix_strerror($errno);
    }
  }
  
  public function attach( $key = null )
  {
    if($key === null )
      $key = $this->key;
    if( $key > 0 )
    {
      $this->key = $key;
//      $this->queue = msg_get_queue($this->key, 0666 | IPC_CREAT);
      $this->queue = msg_get_queue($this->key, 0666 );
      if( !is_resource($this->queue))
        throw new Exception( get_class($this) . "::attach failed to attach to message queue");
//      print "Attached to $key\n";
    } else
      throw new Exception( get_class($this) . "::attach key must be greater than 0");
  }

  // Removes all messages from queue
  public function reset()
  {
    while( msg_receive( $this->queue, 0, $msgType, 16384, $message, false, MSG_IPC_NOWAIT, $errno ) !== FALSE ) ;
  }

  // Removes all messages of the given type from queue
  public function purge( $messageType  )
  {
    $messageType = $messageType === null ? $this->messageType : $messageType;
    while( msg_receive( $this->queue, $messageType, $msgType, 16384, $message, false, MSG_IPC_NOWAIT, $errno ) !== FALSE ) ;
  }

  public function push( $message, $messageType = null )
  {
    $messageType = $messageType === null ? $this->messageType : $messageType;
    $flags = 0;
    $flags |= ($this->blocking ? 0 : MSG_IPC_NOWAIT);
    if( msg_send ($this->queue, $messageType, $message, $this->serialize, $flags, $errno) === FALSE )
      throw new Exception( get_class($this) . "::push " . $this->errno($errno) . " trying to send " . Debug::describe($message), $errno);
    else
      return true;
  }

  public function pop($messageType = null)
  {
    $messageType = $messageType === null ? $this->messageType : $messageType;
    $flags = 0;
    $flags |= ($this->blocking ? 0 : MSG_IPC_NOWAIT);
     if( msg_receive( $this->queue, $messageType, $msgType, 16384, $message, $this->serialize, $flags, $errno ) === FALSE ) 
       throw new Exception( get_class($this) . "::pop " . $this->errno($errno) , $errno);
     else
       return $message;
  }

  public function detach()
  {
     return msg_remove_queue($this->queue); 
  }

  public function stats( $property = null )
  {
    /*
      [msg_perm.uid] => 504
      [msg_perm.gid] => 100
      [msg_perm.mode] => 438
      [msg_stime] => 1155681708
      [msg_rtime] => 1155681708
      [msg_ctime] => 1155681708
      [msg_qnum] => 1
      [msg_qbytes] => 16384
      [msg_lspid] => 21203
      [msg_lrpid] => 21203
     */
    $stats = msg_stat_queue($this->queue);
    
    if( $property === NULL )
      return $stats;
    else
      return $stats[$property];
    
  }

}
 
/*
// Example Usage:
$mq = new MessageQueue( 12345 );
$mq->push("Message #1", 1);
$mq->push("Message #2", 1);
print "Queue size {$mq->size}\n";
while( $mq->size )
  print $mq->pop(1) . "\n";
*/

?>
