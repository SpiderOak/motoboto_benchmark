# -*- coding: utf-8 -*-
"""
base_customer.py

Base class representing a single nimbus.io customer
"""
import logging
import random

import motoboto
from motoboto.s3.key import Key

from lumberyard.http_connection import LumberyardHTTPError, \
        LumberyardRetryableHTTPError
from lumberyard.http_util import compute_default_collection_name

from mock_input_file import MockInputFile
from mock_output_file import MockOutputFile
from bucket_name_manager import BucketNameManager
from key_name_manager import KeyNameManager

class CustomerError(Exception):
    pass

_max_archive_retries = 10
_max_delete_retries = 10

class BaseCustomer(object):
    """
    Base class representing a single nimbus.io customer
    """
    def __init__(self, halt_event, user_identity, test_script):
        self._log = logging.getLogger(user_identity.user_name)
        self._halt_event = halt_event
        self._user_identity = user_identity
        self._test_script = test_script

        self._default_collection_name = compute_default_collection_name(
            self._user_identity.user_name
        )
        self._s3_connection = None

        self._buckets = dict()
        self._keys_by_bucket = dict()

        self._dispatch_table = {
            "create-bucket"     : self._create_bucket,
            "delete-bucket"     : self._delete_bucket,
            "archive-new"       : self._archive_new,
            "archive-replace"   : self._archive_replace,
            "archive-version"   : self._archive_version,
            "retrieve"          : self._retrieve,
            "delete-key"        : self._delete_key,
        }
        self._frequency_table = list()

        self._bucket_name_manager = BucketNameManager(
            self._user_identity.user_name,
            test_script["max-bucket-count"],
        ) 

        self._key_name_manager = KeyNameManager() 
        self._key_name_generator = None

    def _main_loop(self):
        self._s3_connection = motoboto.connect_s3(identity=self._user_identity)

        self._initial_inventory()
        self._load_frequency_table()

        self._key_name_generator = self._key_name_manager.key_name_generator()

        # do an initial delay so all customers don't start at once
        self._delay()

        while not self._halt_event.is_set():
            # run a randomly selected test function
            test_function = self._frequency_table[random.randint(0, 99)]
            test_function()
            self._delay()

        self._s3_connection.close()

    def _initial_inventory(self):
        """get an initial inventory of buckets and files"""
        buckets = self._s3_connection.get_all_buckets()
        for bucket in buckets:
            self._log.info("_initial_inventory found bucket %r" % (
                bucket.name,
            ))
            self._buckets[bucket.name] = bucket
            self._bucket_name_manager.existing_bucket_name(bucket.name)
            keys = bucket.get_all_keys()
            for key in keys:
                self._log.info("_initial_inventory found key %r, %r" % (
                    key.name, bucket.name,
                ))
                if not bucket.name in self._keys_by_bucket:
                    self._keys_by_bucket[bucket.name] = set()
                self._keys_by_bucket[bucket.name].add(key)
                self._key_name_manager.existing_key_name(key.name)

    def _load_frequency_table(self):
        """
        for each action specfied in the distribution append n instances
        of the corresponding function object. We will choose a random number 
        between 0 and 99, to select a test action        
        """
        for key in self._test_script["distribution"].keys():
            count = self._test_script["distribution"][key]
            for _ in xrange(count):
                self._frequency_table.append(self._dispatch_table[key])
        assert len(self._frequency_table) == 100

    def _delay(self):
        """wait for a (delimited) random time"""
        delay_size = random.uniform(
            self._test_script["low-delay"], self._test_script["high-delay"]
        )
        self._halt_event.wait(delay_size)

    def _create_bucket(self):
        if len(self._buckets) >= self._test_script["max-bucket-count"]:
            self._log.info("ignore _create_bucket: already have %s buckets" % (
                len(self._buckets),
            ))
            return
        bucket_name = self._bucket_name_manager.next()
        if bucket_name is None:
            self._log.info("ignore _create_bucket")
            return
        self._log.info("create bucket %r" % (bucket_name, ))
        new_bucket = self._s3_connection.create_bucket(bucket_name)
        self._buckets[new_bucket.name] = new_bucket  

    def _delete_bucket(self):
        eligible_bucket_names = [
            k for k in self._buckets.keys() \
            if k != self._default_collection_name
        ]
        if len(eligible_bucket_names) == 0:
            self._log.warn("no buckets eligible for deletion")
            return

        bucket_name = random.choice(eligible_bucket_names)
        self._log.info("delete bucket %r" % (bucket_name, ))
        bucket = self._buckets.pop(bucket_name)

        self._bucket_name_manager.deleted_bucket_name(bucket_name)

        # delete all the keys for the bucket
        if bucket.name in self._keys_by_bucket:
            for key in self._keys_by_bucket.pop(bucket.name):
                key.delete()

        self._s3_connection.delete_bucket(bucket.name)

    def _archive_new(self):
        bucket = random.choice(self._buckets.values())
        key_name = self._key_name_generator.next()
        self._archive(bucket, key_name)
        
    def _archive_replace(self):
        bucket = random.choice(self._buckets.values())
        if not bucket.name in self._keys_by_bucket:
            self._log.warn("No keys for bucket, skipping _archive_replace")
            return

        key = random.choice(list(self._keys_by_bucket[bucket.name]))
        self._archive(bucket, key.name, replace=True)
        
    def _archive_version(self):
        bucket = random.choice(self._buckets.values())
        if not bucket.name in self._keys_by_bucket:
            self._log.warn("No keys for bucket, skipping _archive_replace")
            return

        key = random.choice(list(self._keys_by_bucket[bucket.name]))
        self._archive(bucket, key.name, replace=False)
        
    def _archive(self, bucket, key_name, replace=True):
        before_stats = bucket.get_space_used() 

        key = Key(bucket)
        key.name = key_name
        size = random.randint(
            self._test_script["min-file-size"],
            self._test_script["max-file-size"]
        )
        self._log.info("archiving %r into %r %s" % (
            key_name, bucket.name, size,
        ))

        input_file = MockInputFile(size)

        retry_count = 0

        while True:

            try:
                key.set_contents_from_file(input_file, replace=replace)
            except LumberyardRetryableHTTPError, instance:
                if retry_count >= _max_archive_retries:
                    raise
                self._log.warn("%s: retry in %s seconds" % (
                    instance, instance.retry_after,
                ))
                self._halt_event.wait(instance.retry_after)
                retry_count += 1
            else:
                break

        after_stats = bucket.get_space_used()

        if after_stats["bytes_added"] != before_stats["bytes_added"] + size:
            self._log.error("%s:%r bytes_added: %s != %s + %s" % (
                key.name, 
                bucket.name, 
                after_stats["bytes_added"],
                before_stats["bytes_added"],
                size, 
            ))

        if not bucket.name in self._keys_by_bucket:
            self._keys_by_bucket[bucket.name] = set()
        self._keys_by_bucket[bucket.name].add(key)

    def _retrieve(self):
        # if we don't have any keys yet, we have to skip this
        if len(self._keys_by_bucket) == 0:
            self._log.warn("skipping _retrieve, no keys yet")
            return
        
        # pick a random key from a random bucket
        key_set = random.choice(self._keys_by_bucket.values())
        key = random.choice(list(key_set))

        before_stats = key._bucket.get_space_used()

        self._log.info("retrieving %r from %r" % (
            key.name, key._bucket.name, 
        ))

        output_file = MockOutputFile()

        try:
            key.get_contents_to_file(output_file)
        except LumberyardHTTPError, instance:
            if instance.status == 404:
                self._log.error("%r not found in %r" % (
                    key.name, key._bucket.name, 
                ))
                return
            raise

        after_stats = key._bucket.get_space_used()

        if after_stats["bytes_retrieved"] != \
           before_stats["bytes_retrieved"] + output_file.bytes_written:
            self._log.error("%s:%r bytes_retrieved: %s != %s + %s" % (
                key.name, 
                key._bucket.name, 
                after_stats["bytes_retrieved"],
                before_stats["bytes_retrieved"],
                output_file.bytes_written, 
            ))

    def _delete_key(self):
        # if we don't have any keys yet, we have to skip this
        if len(self._keys_by_bucket) == 0:
            self._log.warn("skipping _delete_key, no keys yet")
            return
        
        # pop a random key from a random bucket
        bucket_name = random.choice(self._keys_by_bucket.keys())

        key_set = self._keys_by_bucket[bucket_name]
        key = random.choice(list(key_set))
        key_set.remove(key)
        if len(key_set) == 0:
            del self._keys_by_bucket[bucket_name]

        before_stats = key._bucket.get_space_used()

        self._log.info("deleting %r from %r" % (key.name, bucket_name, ))

        retry_count = 0
        while True:

            try:
                key.delete()
            except LumberyardRetryableHTTPError, instance:
                if retry_count >= _max_delete_retries:
                    raise
                self._log.warn("%s: retry in %s seconds" % (
                    instance, instance.retry_after,
                ))
                self._halt_event.wait(instance.retry_after)
                retry_count += 1
            else:
                break

        after_stats = key._bucket.get_space_used()

        # TODO: get key size so we can verify this
        self._log.info("%s:%r bytes_removed: %s %s" % (
            key.name, 
            key._bucket.name, 
            after_stats["bytes_removed"],
            before_stats["bytes_removed"],
        ))

