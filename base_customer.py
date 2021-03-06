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

from mock_input_file import MockInputFile, MockInputFileError
from mock_output_file import MockOutputFile
from bucket_name_manager import BucketNameManager
from key_name_manager import KeyNameManager
from collection_ops_accounting import CollectionOpsAccounting

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
        if "archive-failure-percent" in self._test_script:
            self._archive_failure_percent = \
                self._test_script["archive-failure-percent"]
        else:
            self._archive_failure_percent = 0

        self._default_collection_name = compute_default_collection_name(
            self._user_identity.user_name
        )
        self._s3_connection = None

        self._buckets = dict()
        self._bucket_accounting = dict()
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

        if self._test_script.get("audit-after", False):
            self._audit_after()

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
            if bucket.versioning:
                self._versioned_bucket_names.append(bucket.name)
            else:
                self._unversioned_bucket_names.append(bucket.name)
            self._bucket_accounting[bucket.name] = CollectionOpsAccounting()
            bucket_accounting = self._bucket_accounting[bucket.name]
            bucket_accounting.increment_by("listmatch_request", 1)
            keys = bucket.get_all_keys()
            bucket_accounting.increment_by("listmatch_success", 1)
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
            self._error_count += 1
            self._log.error("_verify_key key not found {0} error #{1}".format(
                verification_key, self._error_count))
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

    def _verify_key_final(self, bucket, key, data_size, md5_digest):
        """
        remove key when verifying, so we can track unmatched keys
        """
        verification_key = (bucket.name, key.name, key.version_id, )
        try:
            expected_data_size, expected_md5_digest = \
                self.key_verification.pop(verification_key)
        except KeyError:
            self._error_count += 1
            self._log.error("key not found {0} error #{1}".format(
                verification_key, self._error_count))
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
            bucket_accounting = self._bucket_accounting[bucket.name]
            if bucket.versioning:
                bucket_accounting.increment_by("listmatch_request", 1)
                for key in bucket.get_all_versions():
                    result = key.get_contents_as_string(
                        version_id=key.version_id
                    )
                    verification_key = (bucket.name, key.name, key.version_id)
                    md5_sum = hashlib.md5(result)
                    if verification_key in self.key_verification:
                        self._log.error("duplicate key (versioning) %s" % (
                            verification_key, ))
                    self.key_verification[verification_key] = \
                        (len(result), md5_sum.digest(), )
                bucket_accounting.increment_by("listmatch_success", 1)
            else:
                bucket_accounting.increment_by("listmatch_request", 1)
                for key in bucket.get_all_keys():
                    result = key.get_contents_as_string()
                    md5_sum = hashlib.md5(result)
                    verification_key = (bucket.name, key.name, key.version_id)
                    if verification_key in self.key_verification:
                        self._log.error("duplicate key %s" % (
                            verification_key, ))
                    self.key_verification[verification_key] = \
                        (len(result), md5_sum.digest(), )
                bucket_accounting.increment_by("listmatch_success", 1)

    def _verify_after(self):
        """
        retrieve all known keys to verify that they are reachable
        check md5 digests if they exist
        """
        self._log.info("verifying retrieves after")
        buckets = self._s3_connection.get_all_buckets()
        for bucket in buckets:
            bucket_accounting = self._bucket_accounting[bucket.name]
            if bucket.versioning:
                bucket_accounting.increment_by("listmatch_request", 1)
                for key in bucket.get_all_versions():
                    result = key.get_contents_as_string(
                        version_id=key.version_id
                    )
                    md5_sum = hashlib.md5(result)
                    self._verify_key_final(bucket, 
                                           key, 
                                           len(result), 
                                           md5_sum.digest())
                bucket_accounting.increment_by("listmatch_success", 1)
            else:
                bucket_accounting.increment_by("listmatch_request", 1)
                for key in bucket.get_all_keys():
                    result = key.get_contents_as_string()
                    md5_sum = hashlib.md5(result)
                    self._verify_key_final(bucket, 
                                           key, 
                                           len(result), 
                                           md5_sum.digest())
            bucket_accounting.increment_by("listmatch_success", 1)

        
        if len(self.key_verification) > 0:
            self._log.info("{0} unreachable keys".format(
                len(self.key_verification)))
            for key, value in self.key_verification.items():
                self._error_count += 1
                self._log.error("unreachable key {0} {1}".format(key, value))
                
    def _audit_after(self):
        """
        retrieve the space_usage for each bucket and compare it to our value
        """
        self._log.info("audit_after begin")
        audit_error_count = 0
        buckets = self._s3_connection.get_all_buckets()
        for bucket in buckets:
            result = bucket.get_space_used()
            if not result["success"]:
                audit_error_count += 1
                self._log.error("audit {0} {1}".format(bucket.name, 
                                                       result["error_message"]))
                continue

            # XXX: can't handle more than one day
            if len(result["operational_stats"]) > 1:
                audit_error_count += 1
                self._log.error("audit {0} need a single day {1}".format(
                    bucket.name, result["operational_stats"]))
                continue

            if len(result["operational_stats"]) == 0:
                self._log.info("audit {0} no server data".format(bucket.name))
                server_audit = {"archive_success" : 0,
                                "success_bytes_in" : 0,
                                "retrieve_success" : 0,
                                "success_bytes_out" : 0,
                                "delete_success" : 0,
                                "listmatch_success" : 0, }
            else:
                server_audit = result["operational_stats"][0]

            our_audit = self._bucket_accounting[bucket.name]
            if our_audit["archive_success"] != server_audit["archive_success"]:
                audit_error_count += 1
                self._log.error("audit {0} archive_success {1} {2}".format(
                    bucket.name,
                    our_audit["archive_success"], 
                    server_audit["archive_success"]))
            if our_audit["success_bytes_in"] != server_audit["success_bytes_in"]:
                audit_error_count += 1
                self._log.error("audit {0} success_bytes_in {1} {2}".format(
                    bucket.name,
                    our_audit["success_bytes_in"], 
                    server_audit["success_bytes_in"]))
            if our_audit["retrieve_success"] != server_audit["retrieve_success"]:
                audit_error_count += 1
                self._log.error("audit {0} retrieve_success {1} {2}".format(
                    bucket.name,
                    our_audit["retrieve_success"], 
                    server_audit["retrieve_success"]))
            if our_audit["success_bytes_out"] != server_audit["success_bytes_out"]:
                audit_error_count += 1
                self._log.error("audit {0} success_bytes_out {1} {2}".format(
                    bucket.name,
                    our_audit["success_bytes_out"], 
                    server_audit["success_bytes_out"]))
            if our_audit["delete_success"] != server_audit["delete_success"]:
                audit_error_count += 1
                self._log.error("audit {0} delete_success {1} {2}".format(
                    bucket.name,
                    our_audit["delete_success"], 
                    server_audit["delete_success"]))
            if our_audit["listmatch_success"] != server_audit["listmatch_success"]:
                audit_error_count += 1
                self._log.error("audit {0} listmatch_success {1} {2}".format(
                    bucket.name,
                    our_audit["listmatch_success"], 
                    server_audit["listmatch_success"]))

        self._log.info("audit_after found {0} errors".format(audit_error_count))
        self._error_count += audit_error_count

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
        new_bucket = self._s3_connection.create_bucket(bucket_name)
        self._buckets[new_bucket.name] = new_bucket  
        self._bucket_accounting[new_bucket.name] = CollectionOpsAccounting()

        return new_bucket

    def _create_bucket(self):
        bucket = self._create_unversioned_bucket()
        if bucket is None:
            return

        self._log.info("create unversioned bucket {0} {1}".format(
            bucket.name, bucket.versioning, ))
        self._unversioned_bucket_names.append(bucket.name)

    def _create_versioned_bucket(self):
        bucket = self._create_unversioned_bucket()
        if bucket is None:
            return

        bucket.configure_versioning(True)
        self._log.info("create versioned bucket {0} {1}".format(
            bucket.name, bucket.versioning, ))
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
        bucket = self._buckets.pop(bucket_name)
        self._log.info("delete bucket {0} versioned={1}".format(
            bucket.name, bucket.versioning))
        self._bucket_accounting[bucket_name].mark_end()

        if bucket.versioning:
            self._clear_versioned_bucket(bucket)
        else:
            self._clear_unversioned_bucket(bucket)

        self._bucket_name_manager.deleted_bucket_name(bucket.name)

        self._s3_connection.delete_bucket(bucket.name)

    def _clear_versioned_bucket(self, bucket):
        try:
            i = self._versioned_bucket_names.index(bucket.name)
        except ValueError:
            self._log.error("not found in versioned buckets {0}".format(
                bucket.name))
        else:
            del self._versioned_bucket_names[i]

        bucket_accounting = self._bucket_accounting[bucket.name]

        # delete all the keys (or versions) for the bucket
        bucket_accounting.increment_by("listmatch_request", 1)
        for key in bucket.get_all_versions():
            verification_key = (bucket.name, key.name, key.version_id)
            self._log.info("_delete_bucket deleting version {0}".format(
                verification_key))
            retry_count = 0
            while not self._halt_event.is_set():

                bucket_accounting.increment_by("delete_request", 1)
                try:
                    key.delete(version_id=key.version_id)
                except LumberyardRetryableHTTPError, instance:
                    bucket_accounting.increment_by("delete_error", 1)
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
            bucket_accounting.increment_by("delete_success", 1)
            try:
                del self.key_verification[verification_key]
            except KeyError:
                self._log.error(
                    "_delete_bucket verification key not found %s" % (
                        verification_key, ))
        bucket_accounting.increment_by("listmatch_success", 1)

    def _clear_unversioned_bucket(self, bucket):
        try:
            i = self._unversioned_bucket_names.index(bucket.name)
        except ValueError:
            self._log.error("not found in unversioned buckets {0}".format(
                bucket.name))
        else:
            del self._unversioned_bucket_names[i]

        bucket_accounting = self._bucket_accounting[bucket.name]
        bucket_accounting.increment_by("listmatch_request", 1)
        for key in bucket.get_all_keys():
            verification_key = (bucket.name, key.name, key.version_id)
            self._log.info("_delete_bucket deleting key {0}".format(
                verification_key))
            retry_count = 0
            while not self._halt_event.is_set():

                bucket_accounting.increment_by("delete_request", 1)
                try:
                    key.delete()
                except LumberyardRetryableHTTPError, instance:
                    bucket_accounting.increment_by("delete_error", 1)
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
            bucket_accounting.increment_by("delete_success", 1)
            try:
                del self.key_verification[verification_key]
            except KeyError:
                self._log.error(
                    "_delete_bucket verification key not found %s" % (
                        verification_key, ))
        bucket_accounting.increment_by("listmatch_success", 1)

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

        bucket_accounting = self._bucket_accounting[bucket.name]
        bucket_accounting.increment_by("listmatch_request", 1)
        keys = bucket.get_all_keys()
        bucket_accounting.increment_by("listmatch_success", 1)

        # if this bucket doesn't have any keys yet, go ahead and add
        # a new one. Otherwise, add a new version of an existing key
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

        bucket_accounting = self._bucket_accounting[bucket.name]
        bucket_accounting.increment_by("listmatch_request", 1)
        keys = bucket.get_all_keys()
        bucket_accounting.increment_by("listmatch_success", 1)

        # if this bucket doesn't have any keys yet, go ahead and add
        # a new one. Otherwise, write over an existing key
        if len(keys) == 0:
            key_name = self._key_name_generator.next()
        else:
            key = random.choice(keys)
            key_name = key.name
            verification_key = (bucket.name, key.name, key.version_id)
            self._log.info("overwriting {0}".format(verification_key))
            try:
                del self.key_verification[verification_key]
            except KeyError:
                self._log.error(
                    "_archive_overwrite verification key not found %s" % (
                        verification_key, ))

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

        self._log.info("initiate multipart %r %r %s" % (
            bucket.name, key_name, size
        ))
        bucket_accounting = self._bucket_accounting[bucket.name]

        retry_count = 0
        while not self._halt_event.is_set():
            bucket_accounting.increment_by("archive_request", 1)
            try:
                multipart_upload = bucket.initiate_multipart_upload(key_name)
            except LumberyardRetryableHTTPError, instance:
                bucket_accounting.increment_by("archive_error", 1)
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
            force_error = random.randint(0, 99) < self._archive_failure_percent
            part_num = i + 1
            retry_count = 0
            while not self._halt_event.is_set():
                input_file = MockInputFile(part_size, force_error=force_error)
                try:
                    multipart_upload.upload_part_from_file(
                        input_file, part_num, replace
                    )
                except MockInputFileError:
                    bucket_accounting.increment_by("archive_error", 1)
                    self._log.info("MockInputFileError")
                    return
                except LumberyardRetryableHTTPError, instance:
                    bucket_accounting.increment_by("archive_error", 1)
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
                bucket_accounting.increment_by("archive_error", 1)
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
        if verification_key in self.key_verification:
            self._log.error("_archive_multipart duplicate key %s" % (
                verification_key, ))
        bucket_accounting.increment_by("archive_success", 1)
        self.key_verification[verification_key] = (size, None, )

    def _archive_one_file( self, bucket, key_name, replace, size, ):
        key = Key(bucket)
        key.name = key_name
        self._log.info("_archive_one_file ({0} {1} ...) versioning={2}".format(
            bucket.name, 
            key.name, 
            bucket.versioning,
        ))
        bucket_accounting = self._bucket_accounting[bucket.name]

        retry_count = 0
        force_error = random.randint(0, 99) < self._archive_failure_percent
        while not self._halt_event.is_set():
            bucket_accounting.increment_by("archive_request", 1)

            input_file = MockInputFile(size, force_error)

            try:
                key.set_contents_from_file(input_file, replace=replace) 
            except MockInputFileError:
                bucket_accounting.increment_by("archive_error", 1)
                self._log.info("MockInputFileError")
                return
            except LumberyardRetryableHTTPError, instance:
                bucket_accounting.increment_by("archive_error", 1)
                if retry_count >= _max_archive_retries:
                    raise
                self._log.warn("%s: retry in %s seconds" % (
                    instance, instance.retry_after,
                ))
                self._halt_event.wait(timeout=instance.retry_after)
                retry_count += 1
                self._log.warn("retry #%s" % (retry_count, ))
                continue

            verification_key = (bucket.name, key.name, key.version_id, )
            self._log.info("archived {0}".format(verification_key))
            if verification_key in self.key_verification:
                self._log.error("_archive_one_file duplicate key %s" % (
                    verification_key, ))
            bucket_accounting.increment_by("archive_success", 1)
            # we count this as 'bytes in' because that's what the server counts
            bucket_accounting.increment_by("success_bytes_in", size)
            self.key_verification[verification_key] = \
                    (size, input_file.md5_digest, )

            break

    def _retrieve_latest(self):
        # pick a random key from a random bucket
        if len(self._unversioned_bucket_names) == 0:
            self._log.warn(
                "_retrieve_latest ignored: noun versioned buckets"
            )
            return
        bucket_name = random.choice(self._unversioned_bucket_names)
        bucket = self._buckets[bucket_name]

        bucket_accounting = self._bucket_accounting[bucket.name]
        bucket_accounting.increment_by("listmatch_request", 1)
        keys = bucket.get_all_keys()
        bucket_accounting.increment_by("listmatch_success", 1)

        if len(keys) == 0:
            self._log.warn("skipping _retrieve_latest, no keys yet")
            return

        key = random.choice(keys)

        self._log.info("retrieving %r from %r" % (
            key.name, key._bucket.name, 
        ))

        output_file = MockOutputFile()

        bucket_accounting.increment_by("retrieve_request", 1)
        try:
            key.get_contents_to_file(output_file)
        except LumberyardHTTPError, instance:
            bucket_accounting.increment_by("retrieve_error", 1)
            if instance.status == 404:
                self._log.error("%r not found in %r" % (
                    key.name, key._bucket.name, 
                ))
                return
            raise

        bucket_accounting.increment_by("retrieve_success", 1)
        # we count this as 'bytes out' because that's what the server counts
        bucket_accounting.increment_by("success_bytes_out",
                                       output_file.bytes_written)
        self._verify_key(bucket, 
                         key, 
                         output_file.bytes_written, 
                         output_file.md5_digest)

    def _retrieve_version(self):
        # pick a random key from the versions of a random bucket
        # XXX: this suppresses the (error?) of finding written over
        # 'versions' of an unversioned file
        if len(self._versioned_bucket_names) == 0:
            self._log.warn(
                "_retrieve_version ignored: no versioned buckets"
            )
            return
        bucket_name = random.choice(self._versioned_bucket_names)
        bucket = self._buckets[bucket_name]
        bucket_accounting = self._bucket_accounting[bucket.name]

        bucket_accounting.increment_by("listmatch_request", 1)
        keys = bucket.get_all_versions()
        bucket_accounting.increment_by("listmatch_success", 1)

        if len(keys) == 0:
            self._log.warn("skipping _retrieve_version, no keys yet")
            return

        key = random.choice(keys)

        self._log.info("retrieving %r %r from %r" % (
            key.name, key.version_id, key._bucket.name, 
        ))

        output_file = MockOutputFile()

        bucket_accounting.increment_by("retrieve_request", 1)
        try:
            key.get_contents_to_file(output_file, version_id=key.version_id)
        except LumberyardHTTPError, instance:
            bucket_accounting.increment_by("retrieve_error", 1)
            if instance.status == 404:
                self._log.error("%r not found in %r" % (
                    key.name, key._bucket.name, 
                ))
                return
            raise

        bucket_accounting.increment_by("retrieve_success", 1)
        # we count this as 'bytes out' because that's what the server counts
        bucket_accounting.increment_by("success_bytes_out",
                                       output_file.bytes_written)
        self._verify_key(bucket, 
                         key, 
                         output_file.bytes_written,
                         output_file.md5_digest)

    def _delete_key(self):
        # pick a random key from a random bucket
        if len(self._unversioned_bucket_names) == 0:
            self._log.warn(
                "_delete_key ignored: no unversioned buckets"
            )
            return
        bucket_name = random.choice(self._unversioned_bucket_names)
        bucket = self._buckets[bucket_name]

        bucket_accounting = self._bucket_accounting[bucket.name]
        bucket_accounting.increment_by("listmatch_request", 1)
        keys = bucket.get_all_keys()
        bucket_accounting.increment_by("listmatch_success", 1)

        if len(keys) == 0:
            self._log.warn("skipping _delete_key, no keys yet")
            return

        key = random.choice(keys)

        self._log.info("_delete_key {0} {1}".format(bucket.name, key.name))

        retry_count = 0
        while not self._halt_event.is_set():

            bucket_accounting.increment_by("delete_request", 1)
            try:
                key.delete()
            except LumberyardRetryableHTTPError, instance:
                bucket_accounting.increment_by("delete_error", 1)
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

        bucket_accounting.increment_by("delete_success", 1)

        # if we delete a key, (not just a version)
        # we need to heave every version we are holding of that key
        delete_list = list()
        for entry in self.key_verification.keys():
            entry_bucket_name, entry_key_name, _ = entry
            if entry_bucket_name == bucket.name and entry_key_name == key.name:
                delete_list.append(entry)

        for verification_key in delete_list:               
            self._log.info("_delete_key: removing {0}".format(
                verification_key))
            del self.key_verification[verification_key]

    def _delete_version(self):
        # pick a random key from the versions of a random bucket
        # XXX: this suppresses the (error?) of finding written over
        # 'versions' of an unversioned file
        if len(self._versioned_bucket_names) == 0:
            self._log.warn(
                "_delete_version ignored: no versioned buckets"
            )
            return
        bucket_name = random.choice(self._versioned_bucket_names)
        bucket = self._buckets[bucket_name]

        bucket_accounting = self._bucket_accounting[bucket.name]
        bucket_accounting.increment_by("listmatch_request", 1)
        keys = bucket.get_all_versions()
        bucket_accounting.increment_by("listmatch_success", 1)

        if len(keys) == 0:
            self._log.warn("skipping _delete_version, no keys yet")
            return

        # we only want to delete a version if there are more than one versions
        # otherwise we are deleting the key
        version_dict = dict()
        for key in keys:
            if key.name in version_dict:
                version_dict[key.name].append(key)
            else:
                version_dict[key.name] = [key, ]

        keys_with_multiple_versions = list()
        for key_name in version_dict.keys():
            if len(version_dict[key_name]) > 1:
                keys_with_multiple_versions.extend(version_dict[key_name])

        if len(keys_with_multiple_versions) == 0:
            self._log.warn(
                "skipping _delete_version, no keys with multiple versions")
            return

        bucket_accounting = self._bucket_accounting[bucket.name]

        key = random.choice(keys_with_multiple_versions)

        verification_key = (bucket.name, key.name, key.version_id)
        self._log.info("deleting version {0}".format(verification_key))

        retry_count = 0
        while not self._halt_event.is_set():
            bucket_accounting.increment_by("delete_request", 1)

            try:
                key.delete(version_id=key.version_id)
            except LumberyardRetryableHTTPError, instance:
                bucket_accounting.increment_by("delete_error", 1)
                if retry_count >= _max_delete_retries:
                    raise
                self._log.warn("%s: retry in %s seconds" % (
                    instance, instance.retry_after,
                ))
                self._halt_event.wait(timeout=instance.retry_after)
                retry_count += 1
                self._log.warn("retry #%s" % (retry_count, ))
            else:
                bucket_accounting.increment_by("delete_success", 1)
                try:
                    del self.key_verification[verification_key]
                except KeyError:
                    self._log.error("_delete_version missing key %s" % (
                        verification_key, ))
                break

