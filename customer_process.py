# -*- coding: utf-8 -*-
"""
customer_process.py

usage: python customer_process.py <test_script_path> <user_identity_path>
"""
import json
import logging
import os
import signal
import sys
from threading import Event

from motoboto.identity import load_identity_from_file

from common import _log_format_template
from base_customer import BaseCustomer

def _create_signal_handler(halt_event):
    def __signal_handler(*_args):
        halt_event.set()
    return __signal_handler

_log_dir = os.environ["NIMBUSIO_LOG_DIR"]

def _initialize_logging(log_name):
    """initialize the log"""
    log_path = os.path.join(_log_dir, log_name)
    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(logging.DEBUG)

def main(test_script_path, user_identity_path):
    """
    main module
    """
    user_identity = load_identity_from_file(user_identity_path)
    _initialize_logging(user_identity.user_name)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

    log.info("loading test script from %r" % (test_script_path, ))
    with open(test_script_path, "rt") as input_file:
        test_script = json.load(input_file)

    customer = BaseCustomer(halt_event, user_identity, test_script)

    try:
        customer._main_loop()
    except Exception, instance:
        log.exception(instance)
        return -1

    log.info("program terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))


