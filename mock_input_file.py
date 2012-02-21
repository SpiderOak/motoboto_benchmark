# -*- coding: utf-8 -*-
"""
mock_input_file.py

An object that acts like an input file, returning a specified number of 
bytes
"""
import hashlib

class MockInputFile(object):
    """
    An object that acts like an input file, returning a specified number of 
    bytes
    """
    def __init__(self, total_size):
        self._total_size = total_size
        self._bytes_read = 0
        self._md5_sum = hashlib.md5()

    def read(self, size=None):
        bytes_remaining = self._total_size - self._bytes_read
        if bytes_remaining == 0:
            return ""

        if size is None or size >= bytes_remaining:
            self._bytes_read = self._total_size
            data = 'a' * bytes_remaining
            self._md5_sum.update(data)
            return data

        self._bytes_read += size
        data = 'a' * size
        self._md5_sum.update(data)
        return data

    @property
    def md5_digest(self):
        return self._md5_sum.digest()

    def __len__(self):
        return self._total_size
