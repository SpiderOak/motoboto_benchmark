# -*- coding: utf-8 -*-
"""
motoboto_benchmark_greelnet_main.py

A process that serves as a client to the SpiderOak lumberyard storage system
Communication is through zeromq
"""
import json
import logging
import os
import os.path
import signal
import sys

import gevent

from gevent.monkey import patch_all
patch_all()

from gevent.event import Event

from motoboto.identity import load_identity_from_file

from common import handle_sigterm, parse_command_line, initialize_logging

from greenlet_customer import GreenletCustomer

def main():
    """
    main processing module
    """
    options = parse_command_line()
    initialize_logging(options.log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    gevent.signal(signal.SIGTERM, handle_sigterm, halt_event)

    log.info("loading test script from %r" % (options.test_script, ))
    with open(options.test_script, "rt") as input_file:
        test_script = json.load(input_file)

    log.info("loading user identity files from %r" % (
        options.user_identity_dir, 
    ))
    customer_list = list()
    for file_name in os.listdir(options.user_identity_dir):
        if options.max_users is not None \
        and len(customer_list) >= options.max_users:
            log.info("breaking at %s users" % (options.max_users, ))
            break

        log.info("loading %r" % (file_name, ))
        user_identity = load_identity_from_file(
            os.path.join(options.user_identity_dir, file_name)
        )
        customer = GreenletCustomer(halt_event, user_identity, test_script)
        customer.start()
        customer_list.append(customer)

    log.info("waiting")
    try:
        halt_event.wait(options.test_duration)
    except KeyboardInterrupt:
        log.info("KeyBoardInterrupt")
        halt_event.set()
    
    log.info("joining")
    for customer in customer_list:
        customer.join()
    
    log.info("program ends")
    return 0

if __name__ == "__main__":
    sys.exit(main())

