# -*- coding: utf-8 -*-
"""
bucket_name_manager.py

generate unique bucket names
"""

from lumberyard.http_util import compute_reserved_collection_name

_bucket_name_template = "%08d"

class BucketNameManager(object):
    """
    generate unique bucket names
    """
    def __init__(self, username):
        self._username = username
        self._bucket_prefix = compute_reserved_collection_name(username, "")
        self._max_bucket_value = 0

    def existing_bucket_name(self, bucket_name):
        """
        parse an existing key to accumulate the max
        """
        if bucket_name.startswith(self._bucket_prefix):
            bucket_value = int(bucket_name[len(self._bucket_prefix):])
            self._max_bucket_value = max(self._max_bucket_value, bucket_value)

    def bucket_name_generator(self):
        while True:
            self._max_bucket_value += 1
            yield compute_reserved_collection_name(
                self._username, 
                _bucket_name_template % (self._max_bucket_value, ) 
            )

