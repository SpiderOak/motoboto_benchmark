# -*- coding: utf-8 -*-
"""
collection_ops_accounting.py

Maintain statisitcs for a collection to compare with the equivalent 
statistics from the server.

See nimbus.io Ticket #64 Implement Operational Stats Accumulation

Note that we accumulate the stats AS THEY APPEAR TO THE SERVER.

For example: 'success_bytes_in' is the number of bytes we SENT to the server.
"""
from datetime import datetime

class CollectionOpsAccounting(object):
    """
    Maintain statisitcs for a collection to compare with the equivalent 
    statistics from the server.
    """
    def __init__(self):
        self._start_timestamp = datetime.utcnow()

        # 2012-11-29 dougfort -- note tham I'm deliberately not using
        # defaultDict because I want to an error if a key is mis-spelled
        self._data = {"retrieve_request" : 0,
                      "retrieve_success" : 0,
                      "retrieve_error" : 0,
                      "archive_request" : 0,
                      "archive_success" : 0,
                      "archive_error" : 0,
                      "listmatch_request" : 0,
                      "listmatch_success" : 0,
                      "listmatch_error" : 0,
                      "delete_request" : 0,
                      "delete_success" : 0,
                      "delete_error" : 0,
                      "socket_bytes_in" : 0,
                      "socket_bytes_out" : 0,
                      "success_bytes_in" : 0,
                      "success_bytes_out" : 0,
                      "error_bytes_in" : 0,
                      "error_bytes_out" : 0, }
        self._end_timestamp = None

    def mark_end(self):
        self._end_timestamp = datetime.utcnow()

    def increment_by(self, key, value):
        self._data[key] += value

