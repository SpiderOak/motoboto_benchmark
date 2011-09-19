# -*- coding: utf-8 -*-
"""
mock_output_file.py

An object that acts like an output file, counting the number of bytes written
"""

class MockOutputFile(object):
    """
    An object that acts like an output file, counting the number of bytes 
    written
    """
    def __init__(self):
        self.bytes_written = 0

    def write(self, data):
        self.bytes_written += len(data) 

