# -*- coding: utf-8 -*-
"""
key_name_manager.py

generate unique keys
"""

_key_name_template = "key-%08d"

class KeyNameManager(object):
    """
    generate unique keys
    """
    def __init__(self):
        self._max_key_value = 0

    def existing_key_name(self, key_name):
        """
        parse an existing key name to accumulate the max
        """
        if key_name.startswith("key-"):
            key_value = int(key_name[4:])
            self._max_key_value = max(self._max_key_value, key_value)

    def key_name_generator(self):
        while True:
            self._max_key_value += 1
            yield _key_name_template % (self._max_key_value, )


