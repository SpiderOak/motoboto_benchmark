# -*- coding: utf-8 -*-
"""
mock_input_file.py

An object that acts like an input file, returning a specified number of 
bytes
"""

class MockInputFile(object):
    """
    An object that acts like an input file, returning a specified number of 
    bytes
    """
    def __init__(self, total_size):
        self._total_size = total_size
        self._bytes_read = 0

    def read(self, size=None):
        bytes_remaining = self._total_size - self._bytes_read
        if bytes_remaining == 0:
            return ""

        if size is None or size >= bytes_remaining:
            self._bytes_read = self._total_size
            return 'a' * bytes_remaining

        self._bytes_read += size
        return 'a' * size

    def __len__(self):
        return self._total_size
