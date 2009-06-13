# encoding: utf-8
import uuid

from django.conf import settings
from django.test.testcases import TestCase

from ..consumer import Consumer
from ..publisher import Publisher

class MQCommunication(TestCase):
    def setUp(self):
        self.assert_(settings.ENABLE_MQ, 'settings.ENABLE_MQ must be True to run message queue tests')
        self.publisher = Publisher(exchange='_hare_test', routing_key='test')
        self.consumer = Consumer(exchange='_hare_test', queue='_hare_test_queue', routing_key='test')
    
    def tearDown(self):
        # Destroy the exchange and queue
        self.consumer.destroy_queue()
        self.publisher.destroy_exchange()
    
    def test_message_integrity(self):
        for i in range(200):
            sent = unicode(uuid.uuid4())
            self.publisher.publish(sent)
            received = self.consumer.pop()
            self.assertEqual(sent, received.body)
    
    def test_unicode(self):
        sent = u'他媽的我的生活 Seru na můj život Ебут мою жизнь FML'
        self.publisher.publish(sent)
        received = self.consumer.pop()
        self.assertEqual(sent, received.body)
