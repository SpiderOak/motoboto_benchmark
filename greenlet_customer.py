# -*- coding: utf-8 -*-
"""
greenlet_customer.py

A greenlet to represent a single nimbus.io customer
"""
from  gevent.greenlet import Greenlet

from base_customer import BaseCustomer

class GreenletCustomer(Greenlet, BaseCustomer):
    """
    A greenlet object to represent a single nimbus.io customer
    """
    def __init__(self, halt_event, user_identity, test_script):
        Greenlet.__init__(self)
        BaseCustomer.__init__(self, halt_event, user_identity, test_script)

    def join(self, timeout=None):
        self._log.info("joining")
        Greenlet.join(self, timeout)

    def _run(self):
        self._main_loop()

    def __str__(self):
       return self._user_identity.user_name

