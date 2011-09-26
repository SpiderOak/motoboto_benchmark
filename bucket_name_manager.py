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
    def __init__(self, username, max_bucket_value):
        self._username = username
        self._bucket_prefix = compute_reserved_collection_name(username, "")
        self._max_bucket_value = max_bucket_value
        self._highest_bucket_value = 0
        self._deleted_bucket_names = list()

    def existing_bucket_name(self, bucket_name):
        """
        parse an existing key to accumulate the max
        """
        if bucket_name.startswith(self._bucket_prefix):
            bucket_value = int(bucket_name[len(self._bucket_prefix):])
            assert bucket_value <= self._max_bucket_value
            self._highest_bucket_value = max(
                self._highest_bucket_value, bucket_value
            )

    def deleted_bucket_name(self, deleted_bucket_name):
        if not deleted_bucket_name in self._deleted_bucket_names:
            self._deleted_bucket_names.append(deleted_bucket_name)

    def next(self):
        if self._highest_bucket_value < self._max_bucket_value:
            self._highest_bucket_value += 1
            return compute_reserved_collection_name(
                self._username, 
                _bucket_name_template % (self._highest_bucket_value, ) 
            )

        if len(self._deleted_bucket_names) > 0:
            return self._deleted_bucket_names.pop()

        return None

