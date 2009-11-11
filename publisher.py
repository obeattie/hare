"""Handles publishing messages to the AMQP server."""
from amqplib import client_0_8 as amqp

from django.utils import simplejson

from .utils.log import logger
from .connection import AMQPConnection

class Publisher(object):
    """A Publisher is responsible for delivering messages to an AMQP exchange. There
       should be at minimum separate publishers per exchange, and possibly a publisher
       per event type, setting the default routing key appropriately."""
    _declared_exchanges = []
    
    def __init__(self, exchange, connection=None, channel=None, routing_key='', **kwargs):
        self.exchange = exchange # default exchange
        self.connection = (connection if connection is not None else AMQPConnection())
        self.channel = (channel if channel is not None else self.connection.channel)
        self.routing_key = routing_key # default routing key
        
        # Now declare the exchange if it hasn't been declared in any other Publisher
        # (if the exchange is declared now, the kwargs passed to this constructor are passed
        # to the exchange declarer)
        if (self.channel is not None) and (self.exchange not in self._declared_exchanges):
            # Unless specified otherwise, the exchange is defaulted to being durable
            # and not automatically deleted
            kwargs.setdefault('durable', True)
            kwargs.setdefault('auto_delete', False)
            # exchange type (passed as exchange_type) defaults to direct
            kwargs['type'] = kwargs.pop('exchange_type', 'direct')
            logger.debug('declaring exchange -> channel.exchange_declare(exchange=%r, %r)' % (exchange, kwargs))
            self.channel.exchange_declare(exchange=exchange, **kwargs)
            self._declared_exchanges.append(exchange)
        
        return super(Publisher, self).__init__()
    
    def destroy_exchange(self):
        """Destroys the exchange this Publisher represents."""
        self.channel.exchange_delete(self.exchange)
        self._declared_exchanges.remove(self.exchange)
    
    def encode(self, body):
        return body
    
    def basic_publish(self, body, **kwargs):
        """Publishes a message to the exchange (a convenience wrapper around
           `Channel.basic_publish` [hence the same name])."""
        # Do nothing if the channel is disabled
        if not self.channel is not None:
            return
        # Take out the keyword arguments which are to be used as arguments
        # to Channel.basic_publish
        publisher_kwargs = {
            'exchange': kwargs.pop('exchange', self.exchange),
            'routing_key': kwargs.pop('routing_key', self.routing_key),
            'mandatory': kwargs.pop('mandatory', False),
            'immediate': kwargs.pop('immediate', False),
            'ticket': kwargs.pop('ticket', None),
        }
        # Set the message to default to being persistent
        kwargs.setdefault('delivery_mode', 2)
        # Create the message
        body = self.encode(body)
        message = amqp.Message(body=body, **kwargs)
        logger.debug('publishing message -> channel.basic_publish(<msg hidden>, %r)' % publisher_kwargs)
        return self.channel.basic_publish(msg=message, **publisher_kwargs)
    publish = basic_publish # convenient alias

class JSONPublisher(Publisher):
    """Publisher that handles pushing JSON-formatted messages."""
    def encode(self, body):
        return simplejson.dumps(body)
