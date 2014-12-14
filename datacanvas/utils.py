# -*- coding: utf-8 -*-

import sys
import time
import os
import subprocess
import boto
from urlparse import urlparse, urlsplit, urlunsplit, urlunparse


def clean_hdfs_path(p):
    if cmd("hadoop fs -rm -r -f %s && hadoop fs -mkdir -p %s" % (p, p)) == 0:
        return True
    else:
        return False


def hdfs_safe_upload(f_local, f_remote):
    f_remote_dir = os.path.dirname(f_remote)
    if cmd("hadoop fs -mkdir -p %s" % f_remote_dir) != 0:
        raise Exception("Failed to create dir %s" % f_remote_dir)
    print("HDFS Upload :: %s ====> %s" % (f_local, f_remote))
    if cmd("hadoop fs -copyFromLocal %s %s" % (f_local, f_remote_dir)) != 0:
        raise Exception("Failed to upload file %s to %s" % (f_local, f_remote_dir))


def cmd(cmd_str):
    print("Execute External Command : '%s'" % cmd_str)
    ret = subprocess.call(cmd_str, shell=True)
    print("Exit with exit code = %d" % ret)
    return ret


def emr_wait_steps(emr_conn, job_flow_id, job_steps):
    for step_id in job_steps.stepids:
        while True:
            ret = emr_conn.describe_step(job_flow_id, step_id.value)
            step_state = ret.status.state
            print "Wait EMR jobflow step: jobflow_id='%s' step_id='%s' state='%s'" % \
                  (job_flow_id, step_id.value, step_state)
            if step_state in ["PENDING", "RUNNING", "CONTINUE"]:
                time.sleep(10)
                continue
            elif step_state in ['COMPLETED']:
                break
            else:
                return False
    return True


def s3_upload(bucket, local_filename, remote_filename):

    # TODO : should refactor this?
    if urlparse(local_filename).scheme in ["s3", "s3n"]:
        return local_filename

    # max size in bytes before uploading in parts.
    # between 1 and 5 GB recommended
    MAX_SIZE = 40 * 1000 * 1000
    # size of parts when uploading in parts
    PART_SIZE = 6 * 1000 * 1000

    fn_local = os.path.normpath(local_filename)
    fn_remote = urlparse(remote_filename).path
    fn_remote_full = remote_filename

    filesize = os.path.getsize(local_filename)
    print("filesize = %d, maxsize = %d" % (filesize, MAX_SIZE))
    if filesize > MAX_SIZE:
        print("Multi-part uploading...")
        print("From : %s" % fn_local)
        print("To   : %s" % fn_remote_full)
        mp = bucket.initiate_multipart_upload(fn_local)
        fp = open(fn_local, 'rb')
        fp_num = 0
        while (fp.tell() < filesize):
            fp_num += 1
            print "uploading part %i" % fp_num
            mp.upload_part_from_file(fp, fp_num, cb=percent_cb, num_cb=10, size=PART_SIZE)
        mp.complete_upload()
        print("")
    else:
        print("Single-part upload...")
        print("From : %s" % fn_local)
        print("To   : %s" % fn_remote_full)
        k = boto.s3.key.Key(bucket)
        k.key = fn_remote
        k.set_contents_from_filename(fn_local, cb=percent_cb, num_cb=10)
        print("")

    return fn_remote_full


def s3_delete(bucket, s3_path):
    from urlparse import urlparse

    print("s3_delete %s" % s3_path)
    prefix_path = urlparse(s3_path).path[1:]
    for key in bucket.list(prefix=prefix_path):
        key.delete()
    return True


def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()


_missing = None


class cached_property(object):
    """A decorator that converts a function into a lazy property.  The
    function wrapped is called the first time to retrieve the result
    and then that calculated result is used the next time you access
    the value::

        class Foo(object):

            @cached_property
            def foo(self):
                # calculate something important here
                return 42

    The class has to have a `__dict__` in order for this property to
    work.
    """

    # implementation detail: this property is implemented as non-data
    # descriptor.  non-data descriptors are only invoked if there is
    # no entry with the same name in the instance's __dict__.
    # this allows us to completely get rid of the access function call
    # overhead.  If one choses to invoke __get__ by hand the property
    # will still work as expected because the lookup logic is replicated
    # in __get__ for manual invocation.

    def __init__(self, func, name=None, doc=None):
        self.__name__ = name or func.__name__
        self.__module__ = func.__module__
        self.__doc__ = doc or func.__doc__
        self.func = func

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        value = obj.__dict__.get(self.__name__, _missing)
        if value is _missing:
            value = self.func(obj)
            obj.__dict__[self.__name__] = value
        return value


def url_path_join(*parts):
    """Normalize url parts and join them with a slash."""
    schemes, netlocs, paths, queries, fragments = zip(*(urlsplit(part) for part in parts))
    scheme, netloc, query, fragment = first_of_each(schemes, netlocs, queries, fragments)
    path = '/'.join(x.strip('/') for x in paths if x)
    return urlunsplit((scheme, netloc, path, query, fragment))


def first_of_each(*sequences):
    return (next((x for x in sequence if x), '') for sequence in sequences)


def s3parse(s3_path):
    pr = urlparse(s3_path)
    if not pr.path:
        pr = pr._replace(path="/")
    return pr


def s3join(s3_path, rel_path):
    pr = urlparse(s3_path)
    pr = pr._replace(path=os.path.join(pr.path, rel_path))
    return urlunparse(pr)
