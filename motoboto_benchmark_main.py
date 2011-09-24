# -*- coding: utf-8 -*-
"""
motoboto_benchmark_main.py

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

# 2011-09-19 dougfort -- gevent monkeypatched sockets don't use /etc/hosts
# which I'm using to gimmick addresses for my local tests
#from gevent import monkey 
#monkey.patch_socket()

import zmq
from gevent.queue import Queue
from gevent.event import Event

from publisher import Publisher
from customer import Customer

_log_path = "motoboto_benchmark.log"
_log_format_template = u'%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_default_test_duration = 60 * 60
_pub_address = os.environ.get(
    "MOTOBOTO_BENCHMARK_PUB_ADDRESS", 
    "ipc:///tmp/motoboto_benchmark-main-pub/socket"
)

def _handle_sigterm(halt_event):
    halt_event.set()

def _prepare_ipc_path(address):
    """
    IPC sockets need an existing file for an address
    """
    path = address[len("ipc://"):]
    dir_name = os.path.dirname(path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    if not os.path.exists(path):
        with open(path, "w") as output_file:
            output_file.write("pork")

def _parse_command_line():
    """Parse the command line, returning an options object"""
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option(
        '-l', "--log-path", dest="log_path", type="string",
        help="full path of the log file"
    )
    parser.add_option(
        '-t', "--test-dir", dest="test_dir", type="string",
        help="path to a directory containing JSON test definition files"
    )
    parser.add_option(
        '-d', "--test-duration", dest="test_duration", type="int",
        help="Number of seconds for the test ro run"
    )

    parser.set_defaults(log_path=_log_path)
    parser.set_defaults(test_duration=_default_test_duration)

    options, _ = parser.parse_args()

    if options.test_dir is None:
        print >> sys.stderr, "You must enter a test dir"
        sys.exit(1)

    return options

def _initialize_logging(log_path):
    """initialize the log"""
    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)-8s %(name)-20s %(message)s")
    console.setFormatter(formatter)
    
    logging.root.addHandler(handler)
    logging.root.addHandler(console)

    logging.root.setLevel(logging.DEBUG)

def main():
    """
    main processing module
    """
    options = _parse_command_line()
    _initialize_logging(options.log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    gevent.signal(signal.SIGTERM, _handle_sigterm, halt_event)

    context = zmq.Context()
    pub_queue = Queue()

    if _pub_address.startswith("ipc://"):
        _prepare_ipc_path(_pub_address)

    publisher = Publisher(halt_event, context, _pub_address, pub_queue)

    publisher.start()

    log.info("loading test definition files from %r" % (options.test_dir, ))
    customer_list = list()
    for file_name in os.listdir(options.test_dir):
        if file_name.endswith(".json"):
            log.info("loading %r" % (file_name, ))
            path = os.path.join(options.test_dir, file_name)
            with open(path, "rt") as input_file:
                test_spec = json.load(input_file)
            customer = Customer(halt_event, test_spec, pub_queue)
            customer.start()
            customer_list.append(customer)

    log.info("waiting")
    try:
        halt_event.wait(options.test_duration)
    except KeyboardInterrupt:
        log.info("KeyBoardInterrupt")
        halt_event.set()
    
    log.info("killing greenlets")
    publisher.kill()
    for customer in customer_list:
        customer.kill()
    
    log.info("joining")
    publisher.join()
    for customer in customer_list:
        customer.join()
    
    context.term()

    log.info("program ends")
    return 0

if __name__ == "__main__":
    sys.exit(main())

