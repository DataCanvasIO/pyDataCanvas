# -*- coding: utf-8 -*-

import os
import sys
import tempfile
import tarfile
import zipfile
from boto.compat import json
import subprocess
import boto
import boto.emr.emrobject
import boto.emr.step
from urlparse import urlparse, urlsplit, urlunsplit, urlunparse, unquote
import urllib
from json import loads as json_loads
import copy
import requests


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


def s3_upload(bucket, local_filename, remote_filename):

    if urlparse(local_filename).scheme in ["s3", "s3n"]:
        return local_filename

    # max size in bytes before uploading in parts.
    # between 1 and 5 GB recommended
    MAX_SIZE = 400 * 1000 * 1000
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
        while fp.tell() < filesize:
            fp_num += 1
            print "uploading part %i" % fp_num
            mp.upload_part_from_file(fp, fp_num, cb=percent_cb, num_cb=10, size=PART_SIZE)
        mp.complete_upload()
        print("Done")
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


def s3join(s3_path, *rel_path):
    pr = urlparse(s3_path)
    pr = pr._replace(path=os.path.normpath(os.path.join(pr.path, *rel_path)))
    return urlunparse(pr)

# TODO: Refactor to 'botocore' when it becomes mature.
def convert_emr_object(obj):
    """Convert a boto emrobject into human readable python dict.

    :param obj: A emrobject
    :return: dict
    """

    if not isinstance(obj, boto.emr.emrobject.EmrObject):
        return obj

    if isinstance(obj, boto.emr.emrobject.Step):
        ret_obj = obj.__dict__
        ret_obj['args'] = [convert_emr_object(arg) for arg in ret_obj['args']]
        del ret_obj['connection']
        return ret_obj
    elif isinstance(obj, boto.emr.emrobject.HadoopStep):
        ret_obj = obj.__dict__
        ret_obj['status'] = convert_emr_object(ret_obj['status'])
        ret_obj['config'] = convert_emr_object(ret_obj['config'])
        del ret_obj['connection']
        return ret_obj
    elif isinstance(obj, boto.emr.emrobject.ClusterStatus):
        ret_obj = obj.__dict__
        ret_obj['timeline'] = convert_emr_object(ret_obj['timeline'])
        ret_obj['statechangereason'] = convert_emr_object(ret_obj['statechangereason'])
        return ret_obj
    elif isinstance(obj, boto.emr.emrobject.ClusterTimeline):
        return obj.__dict__
    elif isinstance(obj, boto.emr.emrobject.StepConfig):
        ret_obj = obj.__dict__
        ret_obj['args'] = [convert_emr_object(arg) for arg in ret_obj['args']]
        return ret_obj
    elif isinstance(obj, boto.emr.emrobject.Arg):
        return obj.value
    elif isinstance(obj, boto.emr.emrobject.JobFlow):
        ret_obj = obj.__dict__
        ret_obj['bootstrapactions'] = [convert_emr_object(b) for b in ret_obj['bootstrapactions']]
        ret_obj['instancegroups'] = [convert_emr_object(b) for b in ret_obj['instancegroups']]
        ret_obj['steps'] = [convert_emr_object(b) for b in ret_obj['steps']]
        del ret_obj['connection']
        return ret_obj
    elif isinstance(obj, boto.emr.emrobject.BootstrapAction):
        ret_obj = obj.__dict__
        ret_obj['args'] = [convert_emr_object(arg) for arg in ret_obj['args']]
        del ret_obj['connection']
        return ret_obj
    elif isinstance(obj, boto.emr.emrobject.InstanceGroup):
        ret_obj = obj.__dict__
        del ret_obj['connection']
        return ret_obj
    elif isinstance(obj, boto.emr.emrobject.ClusterStateChangeReason):
        ret_obj = obj.__dict__
        return ret_obj
    else:
        return str(obj)

    # elif isinstance(obj, boto.emr.emrobject.EmrConnection):
    #     print "EmrConnection"
    #     print dir(obj)
    #     return obj

    return obj


def pprint_aws_obj(obj):
    return json.dumps(convert_emr_object(obj), sort_keys=True, indent=4, separators=(',', ': '))
    # return json.dumps(obj, cls=BotoJsonEncoder)


def pprint_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))


def prepare_hadoop_conf_tarfile(fn_tarfile):
    tf = tarfile.open(fn_tarfile, "r:*")
    ti_dirs = [ti.path for ti in tf if ti.isdir()]
    if len(ti_dirs) > 1:
        raise Exception("Invalid hadoop_conf archive format. It must contain zero or one dir on top level!")

    tmp_hadoop_dir = tempfile.mkdtemp(prefix="hadoop_conf_")
    for member in tf.getmembers():
        if member.isreg():
            tf.extract(member, tmp_hadoop_dir)

    final_hadoop_conf_dir = os.path.join(tmp_hadoop_dir, *ti_dirs)
    return final_hadoop_conf_dir


def zip_isdir(fn):
    return fn.endswith("/")


def prepare_hadoop_conf_zipfile(fn_zipfile):
    zf = zipfile.ZipFile(fn_zipfile)
    zi_dirs = [zi for zi in zf.namelist() if zip_isdir(zi)]
    if len(zi_dirs) > 1:
        raise Exception("Invalid hadoop_conf archive format. It must contain zero or one dir on top level!")

    tmp_hadoop_dir = tempfile.mkdtemp(prefix="hadoop_conf_")

    for zi in zf.infolist():
        zf.extract(zi, tmp_hadoop_dir)

    final_hadoop_conf_dir = os.path.join(tmp_hadoop_dir, *zi_dirs)
    return final_hadoop_conf_dir


def prepare_hadoop_conf(fn, safe=False):
    def _prepare_hadoop_conf_file(fn):

        if tarfile.is_tarfile(fn):
            return prepare_hadoop_conf_tarfile(fn)
        elif zipfile.is_zipfile(fn):
            return prepare_hadoop_conf_zipfile(fn)
        else:
            raise Exception("Can not prepare hadoop_conf archive file: '%s'" % fn)

    def _prepare_hadoop_conf(fn):
        pr = urlparse(fn)
        if pr.scheme in ["http", "https"]:
            with tempfile.NamedTemporaryFile(prefix="tmp_hadoop_conf_") as f:
                try:
                    urllib.urlretrieve(fn, f.name)
                except Exception as e:
                    raise Exception("ERROR : failed to download '%s'" % fn)
                return _prepare_hadoop_conf_file(f.name)
        elif pr.scheme in ["file"]:
            return _prepare_hadoop_conf_file(pr.path)
        elif pr.scheme == '':
            return _prepare_hadoop_conf_file(fn)
        else:
            raise Exception("Do not support file type '%s'" % fn)

    if safe:
        try:
            return _prepare_hadoop_conf(fn)
        except Exception as e:
            print "WARNING: got exception : %s" % e
            return None
    else:
        return _prepare_hadoop_conf(fn)


def preprocess_cluster_envs(base_envs, hadoop_type, cluster_def):
    base_envs = copy.copy(dict(base_envs))

    def _get_new_env(env_name, env_val):

        if isinstance(env_val, dict):
            if 'prepend_path' in env_val:
                ppath = env_val['prepend_path']
                if env_name in base_envs:
                    return ppath + os.pathsep + base_envs.get(env_name)
                else:
                    return ppath
            elif 'append_path' in env_val:
                ppath = env_val['append_path']
                if env_name in base_envs:
                    return base_envs.get(env_name) + os.pathsep + ppath
                else:
                    return ppath
            else:
                # TODO:
                pass
        elif isinstance(env_val, basestring):
            return env_val
        else:
            print "WARNING: Invalid basic env_vars from cluster_def. Only string or dict."
            return None

    if not os.path.isfile(cluster_def):
        print "WARNING: Can not find definition for clusters, use default one."
        return base_envs
    cluster_defs = json_loads(open(cluster_def).read())
    if hadoop_type not in cluster_defs:
        print "WARNING: Can not find definition of cluster '%s', use default one." % hadoop_type
        return base_envs

    for ek, ev in cluster_defs[hadoop_type]["env_vars"].items():
        ev_new = _get_new_env(ek, ev)
        if ev_new:
            # print "%s=%s" % (ek, ev_new)
            base_envs[ek] = ev_new
    return base_envs

def mask_key(s, show=5):
    return s[:show] + '*' * (len(s) - show*2) + s[-show:]


default_headers = {
    'content-type': "application/json",
    'cache-control': "no-cache"
}

