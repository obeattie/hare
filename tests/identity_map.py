from django.conf import settings
from django.test.testcases import TestCase

from ..connection import AMQPConnection
from ..consumer import Consumer
from ..publisher import Publisher

class MQIdentityMap(TestCase):
    """Tests that the `AMQPConnection` class respects the use of the identity map pattern for
       actual over-the-wire-connections."""
    def setUp(self):
        self.assert_(getattr(settings, 'ENABLE_MQ', False), 'settings.ENABLE_MQ must be True to run message queue tests')
    
    def test_connection(self):
        # With default args
        connection1 = AMQPConnection(host='localhost:5672')
        connection2 = AMQPConnection(host='localhost:5672')
        self.assertEqual(id(connection1.connection), id(connection2.connection))
        # With custom args (localhost and 127.0.0.1 are treated differently)
        connection3 = AMQPConnection(host='127.0.0.1:5672')
        connection4 = AMQPConnection(host='127.0.0.1:5672')
        self.assertEqual(id(connection3.connection), id(connection4.connection))
        self.assertNotEqual(id(connection1.connection), id(connection3.connection))
