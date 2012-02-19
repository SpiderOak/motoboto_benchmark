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
        self._versioned_bucket_names = list()
        self._unversioned_bucket_names = list()

        self._dispatch_table = {
            "create-bucket"           : self._create_bucket,
            "create-versioned-bucket" : self._create_versioned_bucket,
            "delete-bucket"           : self._delete_bucket,
            "archive-new-key"         : self._archive_new_key,
            "archive-new-version"     : self._archive_new_version,
            "archive-overwrite"       : self._archive_overwrite,
            "retrieve-latest"         : self._retrieve_latest,
            "retrieve-version"        : self._retrieve_version,
            "delete-key"              : self._delete_key,
            "delete-version"          : self._delete_version,
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
        if self._test_script.get("verify-before", False):
            self._verify_retrieves()
        self._load_frequency_table()

        self._key_name_generator = self._key_name_manager.key_name_generator()

        # do an initial delay so all customers don't start at once
        self._delay()

        while not self._halt_event.is_set():
            # run a randomly selected test function
            test_function = self._frequency_table[random.randint(0, 99)]
            try:
                test_function()
            except Exception:
                self._log.exception("test_function")
            self._delay()

        if self._test_script.get("verify-after", False):
            self._verify_retrieves()

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
                self._key_name_manager.existing_key_name(key.name)

    def _verify_retrieves(self):
        """
        retrieve all known keys to verify that they are reachable
        """
        self._log.info("verifying retrieves")
        buckets = self._s3_connection.get_all_buckets()
        for bucket in buckets:
            if bucket.versioning:
                for key in bucket.get_all_versions():
                    result = key.get_contents_as_string(
                        version_id=key.version_id
                    )
            else:
                for key in bucket.get_all_keys():
                    result = key.get_contents_as_string()

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
        self._halt_event.wait(timeout=delay_size)

    def _create_unversioned_bucket(self):
        if len(self._buckets) >= self._test_script["max-bucket-count"]:
            self._log.info("ignore _create_bucket: already have %s buckets" % (
                len(self._buckets),
            ))
            return None

        bucket_name = self._bucket_name_manager.next()
        if bucket_name is None:
            self._log.info("ignore _create_bucket")
            return
        self._log.info("create bucket %r" % (bucket_name, ))
        new_bucket = self._s3_connection.create_bucket(bucket_name)
        self._buckets[new_bucket.name] = new_bucket  

        return new_bucket

    def _create_bucket(self):
        bucket = self._create_unversioned_bucket()
        if bucket is None:
            return

        self._unversioned_bucket_names.append(bucket.name)

    def _create_versioned_bucket(self):
        bucket = self._create_unversioned_bucket()
        if bucket is None:
            return

        bucket.configure_versioning(True)
        self._versioned_bucket_names.append(bucket.name)

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
        try:
            i = self._unversioned_bucket_names.index(bucket_name)
        except ValueError:
            pass
        else:
            del self._unversioned_bucket_names[i]
        try:
            i = self._versioned_bucket_names.index(bucket_name)
        except ValueError:
            pass
        else:
            del self._versioned_bucket_names[i]
        self._bucket_name_manager.deleted_bucket_name(bucket_name)

        # delete all the keys for the bucket
        for key in bucket.get_all_keys():
            retry_count = 0
            while not self._halt_event.is_set():

                try:
                    key.delete()
                except LumberyardRetryableHTTPError, instance:
                    if retry_count >= _max_delete_retries:
                        raise
                    self._log.warn("%s: retry in %s seconds" % (
                        instance, instance.retry_after,
                    ))
                    self._halt_event.wait(timeout=instance.retry_after)
                    retry_count += 1
                    self._log.warn("retry #%s" % (retry_count, ))
                else:
                    break

            if self._halt_event.is_set():
                self._log.info("halt_event set")
                return

        self._s3_connection.delete_bucket(bucket.name)

    def _archive_new_key(self):
        """
        add a new key to a bucket
        """
        # we assume the user has at least one bucket, the default
        bucket = random.choice(self._buckets.values())
        key_name = self._key_name_generator.next()
        self._archive(bucket, key_name)
        
    def _archive_new_version(self):
        """
        add a new version of an existing key to a bucket
        """
        if len(self._versioned_bucket_names) == 0:
            self._log.warn(
                "_archive_new_version ignored: no versioned buckets"
            )
            return
        bucket_name = random.choice(self._versioned_bucket_names)
        bucket = self._buckets[bucket_name]

        # if this bucket doesn't have any keys yet, go ahead and add
        # a new one. Otherwise, add a new version of an existing key
        keys = bucket.get_all_keys()
        if len(keys) == 0:
            key_name = self._key_name_generator.next()
        else:
            key = random.choice(keys)
            key_name = key.name

        self._archive(bucket, key_name)
        
    def _archive_overwrite(self):
        if len(self._unversioned_bucket_names) == 0:
            self._log.warn(
                "_archive_overwrite ignored: no unversioned buckets"
            )
            return
        bucket_name = random.choice(self._unversioned_bucket_names)
        bucket = self._buckets[bucket_name]

        # if this bucket doesn't have any keys yet, go ahead and add
        # a new one. Otherwise, write over an existing key
        keys = bucket.get_all_keys()
        if len(keys) == 0:
            key_name = self._key_name_generator.next()
        else:
            key = random.choice(keys)
            key_name = key.name

        self._archive(bucket, key_name)
        
    def _archive(self, bucket, key_name, replace=True):
        key = Key(bucket)
        key.name = key_name
        size = random.randint(
            self._test_script["min-file-size"],
            self._test_script["max-file-size"]
        )
        self._log.info("archiving %r into %r %s" % (
            key_name, bucket.name, size,
        ))

        retry_count = 0

        while not self._halt_event.is_set():

            input_file = MockInputFile(size)

            try:
                key.set_contents_from_file(input_file, replace=replace)
            except LumberyardRetryableHTTPError, instance:
                if retry_count >= _max_archive_retries:
                    raise
                self._log.warn("%s: retry in %s seconds" % (
                    instance, instance.retry_after,
                ))
                self._halt_event.wait(timeout=instance.retry_after)
                retry_count += 1
                self._log.warn("retry #%s" % (retry_count, ))
            else:
                break

    def _retrieve_latest(self):
        # pick a random key from a random bucket
        bucket = random.choice(self._buckets.values())
        keys = bucket.get_all_keys()
        if len(keys) == 0:
            self._log.warn("skipping _retrieve_latest, no keys yet")
            return
        key = random.choice(keys)

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

    def _retrieve_version(self):
        # pick a random key from the versions of a random bucket
        bucket = random.choice(self._buckets.values())
        keys = bucket.get_all_versions()
        if len(keys) == 0:
            self._log.warn("skipping _retrieve_version, no keys yet")
            return
        key = random.choice(keys)

        self._log.info("retrieving %r %r from %r" % (
            key.name, key.version_id, key._bucket.name, 
        ))

        output_file = MockOutputFile()

        try:
            key.get_contents_to_file(output_file, version_id=key.version_id)
        except LumberyardHTTPError, instance:
            if instance.status == 404:
                self._log.error("%r not found in %r" % (
                    key.name, key._bucket.name, 
                ))
                return
            raise

    def _delete_key(self):
        # pick a random key from a random bucket
        bucket = random.choice(self._buckets.values())
        keys = bucket.get_all_keys()
        if len(keys) == 0:
            self._log.warn("skipping _delete_key, no keys yet")
            return
        key = random.choice(keys)

        self._log.info("deleting %r from %r" % (key.name, bucket.name, ))

        retry_count = 0
        while not self._halt_event.is_set():

            try:
                key.delete()
            except LumberyardRetryableHTTPError, instance:
                if retry_count >= _max_delete_retries:
                    raise
                self._log.warn("%s: retry in %s seconds" % (
                    instance, instance.retry_after,
                ))
                self._halt_event.wait(timeout=instance.retry_after)
                retry_count += 1
                self._log.warn("retry #%s" % (retry_count, ))
            else:
                break

    def _delete_version(self):
        # pick a random key from the versions of a random bucket
        bucket = random.choice(self._buckets.values())
        keys = bucket.get_all_versions()
        if len(keys) == 0:
            self._log.warn("skipping _retrieve_version, no keys yet")
            return
        key = random.choice(keys)

        self._log.info("deleting %r version %s from %r" % (
            key.name, key.version_id, bucket.name, 
        ))

        retry_count = 0
        while not self._halt_event.is_set():

            try:
                key.delete(version_id=key.version_id)
            except LumberyardRetryableHTTPError, instance:
                if retry_count >= _max_delete_retries:
                    raise
                self._log.warn("%s: retry in %s seconds" % (
                    instance, instance.retry_after,
                ))
                self._halt_event.wait(timeout=instance.retry_after)
                retry_count += 1
                self._log.warn("retry #%s" % (retry_count, ))
            else:
                break

