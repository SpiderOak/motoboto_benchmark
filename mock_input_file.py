# -*- coding: utf-8 -*-
"""
mock_input_file.py

An object that acts like an input file, returning a specified number of 
bytes
"""
import hashlib
from itertools import cycle, islice
from string import printable

class MockInputFileError(Exception):
    pass

class MockInputFile(object):
    """
    An object that acts like an input file, returning a specified number of 
    bytes
    If force_error is set to True, raise MockInputFileError during read
    """
    def __init__(self, total_size, force_error=False):
        self._total_size = total_size
        self._force_error = force_error
        self._bytes_read = 0
        self._md5_sum = hashlib.md5()

        # don't use the resources needed for random data
        self._source = cycle(printable)

    def read(self, size=None):
        bytes_remaining = self._total_size - self._bytes_read
        if bytes_remaining == 0:
            return ""

        if size is None or size >= bytes_remaining:
            if self._force_error:
                raise MockInputFileError()
            self._bytes_read = self._total_size
            data = "".join(islice(self._source, bytes_remaining))
            self._md5_sum.update(data)
            return data

        self._bytes_read += size

        if self._force_error:
            bytes_remaining = self._total_size - self._bytes_read
            if bytes_remaining <= 0:
                raise MockInputFileError()
            
        data = "".join(islice(self._source, size))
        self._md5_sum.update(data)
        return data

    @property
    def md5_digest(self):
        return self._md5_sum.digest()

    def __len__(self):
        return self._total_size
