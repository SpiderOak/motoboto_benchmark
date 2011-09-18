# -*- coding: utf-8 -*-
"""
customer.py

A greenlet to represent a single nimbus.io customer
"""
import logging

from  gevent.greenlet import Greenlet

import motoboto
from motoboto.config import config_template

class Customer(Greenlet):
    """
    A greenlet object to represent a single nimbus.io customer
    """
    def __init__(self, halt_event, test_spec, pub_queue):
        Greenlet.__init__(self)
        self._log = logging.getLogger(test_spec["Username"])
        self._halt_event = halt_event
        self._test_spec = test_spec
        self._pub_queue = pub_queue
        self._s3_connection = None

    def join(self, timeout=None):
        """
        close the _pull socket
        """
        if self._s3_connection is not None:
            self._s3_connection.close()
        Greenlet.join(self, timeout)

    def _run(self):
        # the JSON data comes in as unicode. This does bad things to the key
        config = config_template(
            user_name=self._test_spec["Username"], 
            auth_key_id=self._test_spec["AuthKeyId"], 
            auth_key=str(self._test_spec["AuthKey"])
        )
        self._s3_connection = motoboto.connect_s3(config=config)
        buckets = self._s3_connection.get_all_buckets()

        while not self._halt_event.is_set():
            self._halt_event.wait(1.0)

