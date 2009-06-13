"""Convenience stuff for working with long-running message consumer processes."""
from __future__ import with_statement
import traceback

from django.core.mail import mail_admins
from django.template.loader import render_to_string

from .utils.db import transaction
from .utils.log import logger
import .consumer as consumer

class ConsumerProcess(object):
    queue = NotImplemented
    consumer_class = consumer.JSONConsumer
    consumer_args = {}
    auto_ack = True
    
    def __init__(self, **kwargs):
        assert self.queue is not NotImplemented # not optional
        # Create the consumer
        consumer_args = self.consumer_args.copy()
        consumer_args['queue'] = self.queue
        self.consumer = self.consumer_class(**consumer_args)
        
        return super(ConsumerProcess, self).__init__(**kwargs)
    
    def process_message(self, message):
        """Does the 'meat' of the processing work (must be implemented by
           subclasses)."""
        raise NotImplementedError
    
    def run(self, fault_tolerant=True, **kwargs):
        for message in self.consumer.message_iterator(**kwargs):
            try:
                with transaction(): # commit on success
                    self.process_message(message)
            except NotImplementedError:
                # Subclassing ain't happened
                raise
            except:
                # Email the exception details to the site admins
                tb = traceback.format_exc()
                logger.critical(tb)
                mail_admins(
                    fail_silently=False, # there truly is no hope if this fails
                    subject='Queue worker error',
                    message=render_to_string('email/staff_notifications/worker_error.txt', {
                        'traceback': tb,
                        'message': message,
                        'fault_tolerant': fault_tolerant,
                    })
                )
                logger.info('Traceback has been emailed with mail_admins()')
                if not fault_tolerant:
                    # Not in fault-tolerant mode, so re-raise (which will likely
                    # cause a termination)
                    raise
            else:
                # Processing must have succeeded; auto-acknowledge
                if self.auto_ack:
                    self.consumer.acknowledge(message)
            # rinse and repeat
