"""Handles connections to the AMQP server."""
from amqplib import client_0_8 as amqp

from django.conf import settings

from .utils.log import logger

class AMQPConnection(object):
    """Handles connections to the AMQP server. Instances of this class follow something like the
       identity map pattern, where only one connection will be opened per unique set of parameters."""
    _active_connections = {}
    
    def __init__(self, **kwargs):
        self.connection_signature = self._connection_signature(**kwargs)
        
        # Do nothing if the mq is disabled in the settings
        if getattr(settings, 'ENABLE_MQ', False):
            if self.connection_signature not in self._active_connections:
                # Create a new connection, setting its retain count to 1 (this instance)
                sig = dict(self.connection_signature)
                logger.debug('creating new amqp connection -> amqp.Connection(%r)' % sig)
                self._active_connections[self.connection_signature] = [1, amqp.Connection(**sig)]
            else:
                # A retain count is used for each connection to determine if, when closed,
                # an AMQPConnection may close the underlying connection, too.
                self._active_connections[self.connection_signature][0] += 1
            self.connection = self._active_connections[self.connection_signature][1]
            self.enabled = True
        else:
            self.connection = None
            self.enabled = False
        
        # Keeps track of every channel created by this instance
        self._channels = []
        
        return super(AMQPConnection, self).__init__()
    
    def __getattr__(self, name):
        return getattr(self.connection, name)
    
    def __eq__(self, other):
        return self.connection_signature == getattr(other, 'connection_signature', None)
    
    def close(self, *args, **kwargs):
        """Closes any channels and possibly the connection, too. The connection
           will only be closed if there are no other AMQPConnection instances that
           reference it."""
        # Close all channels
        for channel in self._channels:
            logger.debug('closing channel -> channel.close(%r, %r)' % (args, kwargs))
            channel.close(*args, **kwargs)
        # If this is the only AMQPConnection using the Connection, close it too
        if self.enabled:
            self._active_connections[self.connection_signature][0] -= 1
            if self._active_connections[self.connection_signature][0] <= 0:
                logger.debug('closing connection -> connection.close()')
                self.connection.close()
                del self._active_connections[self.connection_signature]
    
    def new_channel(self, *args, **kwargs):
        """Creates a new communication Channel bound to this Connection, passing
           the arguments specified to the Channel constructor."""
        logger.debug('creating channel -> connection.channel(%r, %r)' % (args, kwargs))
        if self.enabled:
            channel = self.connection.channel(*args, **kwargs)
            self._channels.append(channel)
            return channel
        else:
            return None
    
    @property
    def channel(self):
        """Gets a 'default' Channel for the connection. Only one channel is created
           per AMQPConnection instance (note that this is not necessarily the same
           as one Channel per actual connection, but Channels are cheap anyway)."""
        if not hasattr(self, '_channel'):
            self._channel = self.new_channel()
        return self._channel
    
    def _connection_signature(self, host=None, user=None, password=None,
                              vhost=None, insist=False, **kwargs):
        """Returns a 'connection signature', which is basically 'flattened' dictionary of arguments
           (tuple of 2-tuples) to be passed to the client's constructor. This is just a
           convenience function to provide defaults from the settings file if they haven't
           been overridden specifically as keyword arguments to the constructor."""
        host = (host if host is not None else getattr(settings, 'AMQP_HOST', 'localhost:5672'))
        user = (user if user is not None else getattr(settings, 'AMQP_USER', 'guest'))
        password = (password if password is not None else getattr(settings, 'AMQP_PASSWORD', 'guest'))
        vhost = (vhost if vhost is not None else getattr(settings, 'AMQP_VHOST', '/'))
        
        # Just update kwargs with our values and return that
        kwargs.update({
            'host': host,
            'userid': user,
            'password': password,
            'virtual_host': vhost,
            'insist': insist,
        })
        # Have to return a tuple here as we need the resulting object to be hashable
        # (so it can be used to check equality of signatures)
        return tuple(sorted(kwargs.items(), lambda x, y: cmp(x[0], y[0])))
