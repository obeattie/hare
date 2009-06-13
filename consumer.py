"""Handles consuming messages from the AMQP server."""
from __future__ import with_statement

import threading
from amqplib import client_0_8 as amqp

from django.utils import simplejson

from .utils.log import logger
from .connection import AMQPConnection

class Consumer(object):
    _declared_queues = []
    
    def __init__(self, queue, exchange=None, routing_key=None, connection=None,
                 channel=None, force_no_declare=False, tag=None, **kwargs):
        # exchange is not required (but is recommended). Without it, the queue must
        # have already been declared manually.
        self.queue = queue
        self.exchange = exchange
        self.routing_key = routing_key
        self.connection = (connection if connection is not None else AMQPConnection())
        self.channel = (channel if channel is not None else self.connection.channel)
        
        if self.queue not in self._declared_queues:
            if not force_no_declare:
                # Declare the queue...
                assert (self.exchange is not None), 'exchange is required to declare the queue'
                queue_kwargs = {
                    'queue': self.queue,
                    'passive': kwargs.pop('passive', False),
                    'durable': kwargs.pop('durable', True),
                    'exclusive': kwargs.pop('exclusive', False),
                    'auto_delete': kwargs.pop('auto_delete', False),
                    'nowait': kwargs.pop('nowait', False),
                    'arguments': kwargs.pop('queue_arguments', None),
                    'ticket': kwargs.pop('ticket', None),
                }
                logger.debug('declaring queue -> channel.queue_declare(%r)' % queue_kwargs)
                self.channel.queue_declare(**queue_kwargs)
                # ...and bind it to the exchange
                bind_kwargs = {
                    'queue': self.queue,
                    'exchange': self.exchange,
                    'routing_key': (routing_key if routing_key is not None else ''),
                    'nowait': kwargs.pop('nowait', False),
                    'arguments': kwargs.pop('bind_arguments', None),
                    'ticket': kwargs.pop('ticket', None),
                }
                logger.debug('binding queue -> channel.queue_bind(%r)' % bind_kwargs)
                self.channel.queue_bind(**bind_kwargs)
            # Add to _declared_queues so it only gets declared once
            self._declared_queues.append(self.queue)
        
        return super(Consumer, self).__init__()
    
    def destroy_queue(self):
        """Destroys the queue this consumer represents."""
        self.channel.queue_delete(self.queue)
        self._declared_queues.remove(self.queue)
    
    def __iter__(self):
        try:
            while True:
                yield self.pop()
        except IndexError:
            return
    
    def decode(self, message):
        """A hook so that subclasses may decode messages into their own formats. Note that decoders
           should just manipulate the message passed (usually only changing message.body), since the
           other attributes need to be preserved."""
        return message
    
    def pop(self):
        """Pops the next waiting message from the queue, raising IndexError (just
           like the standard Python pop() functions) if none are waiting."""
        logger.debug('popping message -> channel.basic_get(%r)' % self.queue)
        message = self.channel.basic_get(self.queue)
        if not message:
            raise IndexError
        return self.decode(message)
    
    def acknowledge(self, message):
        """Acknowledges delivery of the passed Message instance."""
        logger.debug('acknowledging message -> channel.basic_ack(delivery_tag=%r)' % message.delivery_tag)
        self.channel.basic_ack(delivery_tag=message.delivery_tag)
    
    def subscribe(self, callback):
        """Calls the callback passed whenever a new message is available. Once invoked, this
           function will never return.
           
           Callback do not themselves need to acknowledge receipt of a message. The callback is
           actually wrapped in another function which will acknowledge the message on successful
           execution of the callback function passed."""
        
        # A class wrapper is used so more than just the Message is available when it is called
        # (we need the access to this Consumer and the original callback).
        class FakeCallback(object):
            def __new__(self, *args, **kwargs):
                args = list(args) # tuples are immutable, so have to convert temporarily to list
                args[0] = self.consumer.decode(args[0]) # ...decode the message
                args = tuple(args) # ...and then back to a tuple
                
                # Call the callback, acknowledge success of the message if it succeeds, and return
                # the callback's response.
                logger.debug('calling message callback -> %r(%r, %r)' % (self.callback, args, kwargs))
                ret = self.callback(*args, **kwargs)
                self.consumer.acknowledge(message=args[0])
                return ret
        FakeCallback.consumer = self
        FakeCallback.callback = staticmethod(callback)
        
        logger.debug('subscribing to new messages -> channel.basic_consume(queue=%r...)' % self.queue)
        consumer_tag = self.channel.basic_consume(queue=self.queue, no_ack=False, callback=FakeCallback)
        try:
            while True:
                self.connection.channel.wait()
        finally:
            logger.debug('cancelling message subscription -> channel.basic_cancel(consumer_tag=%r)' % consumer_tag)
            self.channel.basic_cancel(consumer_tag=consumer_tag)
    
    def message_iterator(self, no_ack=False, limit=None):
        """Returns a generator that yields new messages as they are popped off the message queue.
           Will yield messages forever, waiting for new messages to become available if non are already
           in the queue.
           
           Messages will need to be acknowledged manually, through Consumer.acknowledge(), unless
           no_ack is True."""
        # --- Locks and events --- #
        event_lock = threading.Lock()         # acquired with manipulating events
        message_available = threading.Event() # set when a message is available for processing
        waiting = threading.Event()           # set when messages are being waited for (to avoid message loss)
        waiting.set()
        
        class FakeCallback(object):
            def __new__(self, message):
                logger.debug('message received (routing_key=%r). passing back to main control through event.' % getattr(message, 'routing_key', None))
                with self.event_lock:
                    self.waiting.wait()
                    self.message_available.result = message
                    self.message_available.set()
                return None
        FakeCallback.event_lock = event_lock
        FakeCallback.message_available = message_available
        FakeCallback.waiting = waiting
        
        # This is a little intense, a lotta coordination :)
        logger.debug('subscribing to new messages -> channel.basic_consume(queue=%r, no_ack=%r...)' % (self.queue, no_ack))
        tag = self.channel.basic_consume(queue=self.queue, no_ack=no_ack, callback=FakeCallback)
        iterations = 0
        try:
            while True:
                # optionally apply the limit
                iterations += 1
                if limit is not None and iterations >= limit:
                    raise StopIteration
                # limit passed okay...
                self.channel.wait() # wait for the next message
                if message_available.isSet(): # a new message is available to be yielded
                    logger.debug('retrieving message from event.')
                    with event_lock:
                        waiting.clear()
                        logger.debug('yielding message.')
                        yield self.decode(message_available.result)
                        logger.debug('message loop continuing.')
                        # moving on, garbage the previous message and reset the locks
                        del message_available.result
                        message_available.clear()
                        waiting.set()
        except StopIteration:
            return
        finally:
            # The consumer must always be cancelled
            logger.debug('cancelling message subscription -> channel.basic_cancel(consumer_tag=%r)' % tag)
            self.channel.basic_cancel(consumer_tag=tag)

class JSONConsumer(Consumer):
    """Consumer which handles consuming JSON-encoded messages."""
    def decode(self, message):
        message.body = simplejson.loads(message.body)
        return message
