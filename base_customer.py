# -*- coding: utf-8 -*-
"""
base_customer.py

Base class representing a single nimbus.io customer
"""
import hashlib
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
class VerificationError(CustomerError):
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
        self._multipart_upload_cutoff = \
                2 * self._test_script["multipart-part-size"]

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

        self.key_verification = dict()
        self._error_count = 0

    @property
    def error_count(self):
        return self._error_count

    def _main_loop(self):
        self._s3_connection = motoboto.connect_s3(identity=self._user_identity)

        self._initial_inventory()
        if self._test_script.get("verify-before", False):
            self._verify_before()
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
                self._error_count += 1
                self._log.exception("test_function error #{0}".format(
                    self._error_count))
            self._delay()

        if self._test_script.get("verify-after", False):
            self._verify_after()

        self._s3_connection.close()
        self._log.info("{0} errors".format(self._error_count))

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

    def _verify_key(self, bucket, key, data_size, md5_digest):
        verification_key = (bucket.name, key.name, key.version_id, )
        try:
            expected_data_size, expected_md5_digest = \
                    self.key_verification[verification_key]
        except KeyError:
            return

        if data_size != expected_data_size:
            self._error_count += 1
            self._log.error("size mistmatch {0} {1} {2} error #{3}".format(
                data_size,
                expected_data_size,
                verification_key, 
                self._error_count))

        if expected_md5_digest is not None and \
           md5_digest != expected_md5_digest:
            self._error_count += 1
            self._log.error("md5 mismatch {0} error #{1}".format(
                    verification_key, self._error_count))

    def _verify_before(self):
        """
        retrieve all known keys to verify that they are reachable
        store md5 digests 
        """
        self._log.info("verifying retrieves before")
        buckets = self._s3_connection.get_all_buckets()
        for bucket in buckets:
            if bucket.versioning:
                for key in bucket.get_all_versions():
                    result = key.get_contents_as_string(
                        version_id=key.version_id
                    )
                    verification_key = (bucket.name, key.name, key.version_id)
                    md5_sum = hashlib.md5(result)
                    self.key_verification[verification_key] = \
                        (len(result), md5_sum.digest(), )
            else:
                for key in bucket.get_all_keys():
                    result = key.get_contents_as_string()
                    md5_sum = hashlib.md5(result)
                    verification_key = (bucket.name, key.name, key.version_id)
                    self.key_verification[verification_key] = \
                        (len(result), md5_sum.digest(), )

    def _verify_after(self):
        """
        retrieve all known keys to verify that they are reachable
        check md5 digests if they exist
        """
        self._log.info("verifying retrieves before")
        buckets = self._s3_connection.get_all_buckets()
        for bucket in buckets:
            if bucket.versioning:
                for key in bucket.get_all_versions():
                    result = key.get_contents_as_string(
                        version_id=key.version_id
                    )
                    md5_sum = hashlib.md5(result)
                    self._verify_key(bucket, 
                                     key, 
                                     len(result), 
                                     md5_sum.digest())
            else:
                for key in bucket.get_all_keys():
                    result = key.get_contents_as_string()
                    md5_sum = hashlib.md5(result)
                    self._verify_key(bucket, 
                                     key, 
                                     len(result), 
                                     md5_sum.digest())

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
        size = random.randint(
            self._test_script["min-file-size"],
            self._test_script["max-file-size"]
        )

        if size > self._multipart_upload_cutoff:
            self._archive_multipart(bucket, key_name, replace, size)
        else:
            self._archive_one_file(bucket, key_name, replace, size)

    def _archive_multipart(self, bucket, key_name, replace, size):

        # divide up the size into chunks >= part-isze
        base_size = self._test_script["multipart-part-size"]
        part_sizes = [base_size for _ in range(size / base_size)]
        part_sizes[-1] += size % base_size

        retry_count = 0
        while not self._halt_event.is_set():
            try:
                multipart_upload = bucket.initiate_multipart_upload(key_name)
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

        self._log.info("archive multipart %r %r %s %s" % (
            bucket.name, key_name, multipart_upload.id, size
        ))

        # TODO: do this in parallel
        for i, part_size in enumerate(part_sizes):
            part_num = i + 1
            retry_count = 0
            while not self._halt_event.is_set():
                input_file = MockInputFile(part_size)
                try:
                    multipart_upload.upload_part_from_file(
                        input_file, part_num, replace
                    )
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

        retry_count = 0
        while not self._halt_event.is_set():
            try:
                multipart_upload.complete_upload()
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

        verification_key = (bucket.name, key_name, multipart_upload.id, )
        self.key_verification[verification_key] = (size, None, )

    def _archive_one_file( self, bucket, key_name, replace, size, ):
        key = Key(bucket)
        key.name = key_name
        self._log.info("archiving %r into %r replace=%s %s" % (
            key_name, 
            bucket.name, 
            replace, 
            size, 
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
                continue

            self._log.info("%r into %r %s version_id = %s" % (
                key_name, 
                bucket.name, 
                size,
                key.version_id, 
            ))
            verification_key = (bucket.name, key_name, key.version_id, )
            self.key_verification[verification_key] = \
                    (size, input_file.md5_digest, )

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

        self._verify_key(bucket, 
                         key, 
                         output_file.bytes_written, 
                         output_file.md5_digest)

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

        self._verify_key(bucket, 
                         key, 
                         output_file.bytes_written,
                         output_file.md5_digest)

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

