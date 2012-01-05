# -*- coding: utf-8 -*-
"""
motoboto_benchmark_subprocess_main.py

Manage 
"""
import logging
import os
import os.path
import signal
import subprocess
import sys
from threading import Event

from common import parse_command_line, initialize_logging

def _create_signal_handler(halt_event):
    def __signal_handler(*_args):
        halt_event.set()
    return __signal_handler

def main():
    """
    main processing module
    """
    options = parse_command_line()
    initialize_logging(options.log_name)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

    log.info("using test script %r" % (options.test_script, ))

    program_dir = os.path.expandvars("${HOME}/motoboto_benchmark")
    program_path = os.path.join(program_dir, "customer_process.py")

    customer_process_list = list()
    for file_name in os.listdir(options.user_identity_dir):
        if options.max_users is not None \
        and len(customer_process_list) >= options.max_users:
            log.info("breaking at %s users" % (options.max_users, ))
            break

        log.info("user identity %r" % (file_name, ))
        user_identity_path = os.path.join(options.user_identity_dir, file_name)

        args = [
            sys.executable,
            program_path,
            options.test_script,
            user_identity_path
        ]

        environment = {
            "PYTHONPATH"                : os.environ["PYTHONPATH"],
            "NIMBUSIO_LOG_DIR"          : os.environ["NIMBUSIO_LOG_DIR"],
            "NIMBUS_IO_SERVICE_HOST"    : os.environ["NIMBUS_IO_SERVICE_HOST"], 
            "NIMBUS_IO_SERVICE_PORT"    : os.environ["NIMBUS_IO_SERVICE_PORT"], 
            "NIMBUS_IO_SERVICE_DOMAIN"  : \
                os.environ["NIMBUS_IO_SERVICE_DOMAIN"], 
            "NIMBUS_IO_SERVICE_SSL"     : os.environ.get(
                "NIMBUS_IO_SERVICE_SSL", "0"
            )
        }        

        process = subprocess.Popen(args, env=environment)
        customer_process_list.append(process)

    log.info("waiting")
    try:
        halt_event.wait(options.test_duration)
    except KeyboardInterrupt:
        log.info("KeyBoardInterrupt")
        halt_event.set()
    
    log.info("terminating processes")
    for process in customer_process_list:
        process.terminate()

    log.info("waiting for processes")
    for process in customer_process_list:
        process.wait()
        if process.returncode != 0:
            log.error("process returncode %s" % (process.returncode, ))
    
    log.info("program ends")
    return 0

if __name__ == "__main__":
    sys.exit(main())

