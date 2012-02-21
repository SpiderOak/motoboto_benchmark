# -*- coding: utf-8 -*-
"""
mock_output_file.py

An object that acts like an output file, counting the number of bytes written
"""
import hashlib

class MockOutputFile(object):
    """
    An object that acts like an output file, counting the number of bytes 
    written
    """
    def __init__(self):
        self.bytes_written = 0
        self._md5_sum = hashlib.md5()

    def write(self, data):
        self._md5_sum.update(data)
        self.bytes_written += len(data) 

    @property
    def md5_digest(self):
        return self._md5_sum.digest()

