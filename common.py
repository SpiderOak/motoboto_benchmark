# -*- coding: utf-8 -*-
"""
common routines
"""
import logging
import sys

_log_path = "motoboto_benchmark.log"
_log_format_template = u'%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_default_test_duration = 60 * 60

def handle_sigterm(halt_event):
    halt_event.set()

def parse_command_line():
    """Parse the command line, returning an options object"""
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option(
        '-l', "--log-path", dest="log_path", type="string",
        help="full path of the log file"
    )
    parser.add_option(
        '-u', "--user-identity-dir", dest="user_identity_dir", type="string",
        help="path to a directory containing user identity files"
    )
    parser.add_option(
        '-m', "--max-users", dest="max_users", type="int",
        help="maximum number of users, None == use all"
    )
    parser.add_option(
        '-s', "--test-script", dest="test_script", type="string",
        help="path to JSON test script file"
    )
    parser.add_option(
        '-d', "--test-duration", dest="test_duration", type="int",
        help="Number of seconds for the test ro run"
    )

    parser.set_defaults(log_path=_log_path)
    parser.set_defaults(test_duration=_default_test_duration)

    options, _ = parser.parse_args()

    if options.user_identity_dir is None:
        print >> sys.stderr, "You must enter a user identity dir"
        sys.exit(1)

    if options.test_script is None:
        print >> sys.stderr, "You must enter the path to a test script file"
        sys.exit(1)

    return options

def initialize_logging(log_path):
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

