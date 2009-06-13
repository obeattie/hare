"""Ghetto-fab logging module wrapper.
   
   If the setting HARE_LOGGER is defined in your settings.py file, then that logger will
   be used for Hare to log to. Otherwise, a logger which logs to syslog is created, whose
   destination can be optionally controlled by the SYSLOG_ADDRESS setting."""
import logging
from logging.handlers import SysLogHandler

from django.conf import settings

if hasattr(settings, 'HARE_LOGGER'):
    # Use the logger defined in settings.py
    logger = settings.HARE_LOGGER
else:
    # Create a new one
    logger = logging.getLogger('hare')
    logger.setLevel(logging.DEBUG)
    address = getattr(settings, 'SYSLOG_ADDRESS', '/dev/log')
    logger.addHandler(SysLogHandler(address))
