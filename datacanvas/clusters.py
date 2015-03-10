# -*- coding: utf-8 -*-

import os
import subprocess
import time
import boto
import boto.s3
import boto.s3.key
import boto.emr
from boto.emr.step import HiveStep, JarStep, PigStep
from itertools import izip
from datacanvas.utils import cached_property, url_path_join, s3join, \
    s3parse, s3_upload, pprint_aws_obj, prepare_hadoop_conf, \
    preprocess_cluster_envs
from urlparse import urlparse
import abc


class BaseCluster(object):
    """A BaseCluster class define abstract methods for some type of cluster"""
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass

    def check_variables(self):
        pass

    @abc.abstractmethod
    def prepare(self, hadoop_type, **cluster_kws):
        pass

    @abc.abstractmethod
    def get_working_root(self, cluster_params, global_params):
        pass

    @abc.abstractmethod
    def upload_working_file(self, working_root, file_path):
        pass

    @abc.abstractmethod
    def upload_working_dir(self, working_root, local_path):
        pass

    @abc.abstractmethod
    def clean_working_dir(self, dir_path):
        pass

    @abc.abstractmethod
    def prepare_working_file(self, working_root, file_path):
        pass

    @abc.abstractmethod
    def execute_jar(self, job_name, jar_path, jar_args, main_class, *args, **kwargs):
        pass

    # TODO: execute_stream_jar
    def execute_stream_jar(self, jar_file, jar_args, *args, **kwargs):
        pass

    @abc.abstractmethod
    def execute_hive(self, job_name, hive_script, *args, **kwargs):
        pass

    @abc.abstractmethod
    def execute_pig(self, job_name, pig_script, *args, **kwargs):
        pass


class EmrCluster(BaseCluster):
    """A class to access EMR cluster resources.

    Examples:
        aws_key = "XXXXXXXXXXXXXXXXXXXX"
        aws_sec = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        job_id  = "j-xxxxxxxxxxxx"
        step_id = "s-xxxxxxxxxxxx"

        emr_cluster = EmrCluster(job_id, aws_region, aws_key, aws_sec)

        # Get 'stdout' log file
        emr_cluster.get_step_log(step_id)

        # Get 'stderr' log file
        emr_cluster.get_step_log(step_id， logfile="stderr")
    """

    def __init__(self, aws_region, aws_key, aws_secret, jobflow_id=None):
        super(EmrCluster, self).__init__()
        self.jobflow_id = jobflow_id
        self.aws_region = aws_region
        self.aws_key = aws_key
        self.aws_secret = aws_secret

    @cached_property
    def emr_conn(self):
        return boto.emr.connect_to_region(self.aws_region,
                                          aws_access_key_id=self.aws_key,
                                          aws_secret_access_key=self.aws_secret)

    @cached_property
    def s3_conn(self):
        return boto.connect_s3(self.aws_key, self.aws_secret)

    @cached_property
    def emr_info(self):
        return self.emr_conn.describe_cluster(self.jobflow_id)

    @property
    def emr_steps(self):
        steps = self.emr_conn.list_steps(self.jobflow_id)

        all_steps = [{"index": len(steps.steps) - si, "id": se.id, "name": se.name}
                     for si, se in enumerate(steps.steps)]
        steps_dict = {s["id"]: s for s in all_steps}
        return steps_dict

    def s3_list_files(self, path):
        pr = s3parse(path)
        if pr.scheme not in ["s3", "s3n"]:
            raise ValueError("Not valid s3 path: '%s'" % path)
        bucket = self.s3_conn.get_bucket(pr.netloc)
        prefix_path = pr.path[1:]
        return [k.name.encode('utf-8') for k in bucket.list(prefix=prefix_path, delimiter="/")]

    def s3_get_file(self, s3_path):
        pr = s3parse(s3_path)
        if pr.scheme not in ["s3", "s3n"]:
            raise ValueError("Not valid s3 path: '%s'" % s3_path)
        bucket = self.s3_conn.get_bucket(pr.netloc)
        return bucket.get_key(pr.path)

    def s3_delete_file(self, s3_path):
        pr = s3parse(s3_path)
        if pr.scheme not in ["s3", "s3n"]:
            raise ValueError("Not valid s3 path: '%s'" % s3_path)
        bucket = self.s3_conn.get_bucket(pr.netloc)
        prefix_path = pr.path[1:]
        for key in bucket.list(prefix=prefix_path):
            key.delete()
        return True

    def dump_logs(self, step_id, log_files=None, retry_count=1, retry_interval=60, extra_wait_count=5):
        if not log_files:
            return

        while retry_count > 0:
            remote_log_files = [os.path.basename(i)
                                for i in self.s3_list_files(self.emr_step_log_filename(step_id))]
            intersection_files = set(log_files) & set(remote_log_files)
            if not intersection_files:
                print "Seems logs are not ready on s3, retry..."
                retry_count -= 1
                time.sleep(retry_interval)
                continue
            for i in range(extra_wait_count):
                remote_log_files = [os.path.basename(i)
                                    for i in self.s3_list_files(self.emr_step_log_filename(step_id))]
                if set(remote_log_files) >= {"controller", "stdout", "stderr"}:
                    break
                print "Fetching s3 logs..."
                time.sleep(retry_interval)
            for log_file in log_files:
                log_path = self.emr_step_log_filename(step_id, log_file)
                print "==================================="
                print "Dump EMR log file (step=%s): %s" % (step_id, log_file)
                print "Log Path : %s" % log_path
                print "==================================="
                if self.s3_list_files(log_path):
                    print self.emr_step_log(step_id, log_file=log_file)
                else:
                    print "Log file does not exist"
            return

    def prepare(self, hadoop_type, **cluster_kws):
        if hadoop_type not in ["EMR", "EMR_SPOT"]:
            raise Exception("EmrCluster :: Can NOT prepare '%s'" % hadoop_type)

    def get_working_root(self, cluster_params, global_params):
        remote_path = "s3n://{working_bucket}/zetjob/{username}/job{job_id}/blk{blk_id}/".format(
            working_bucket=cluster_params["S3_BUCKET"],
            username=global_params["userName"],
            job_id=global_params["jobId"],
            blk_id=global_params["blockId"]
        )
        return remote_path

    def upload_working_file(self, working_root, file_path):
        uploaded_files = self.upload_working_dir(working_root, file_path)
        return uploaded_files[0] if uploaded_files else None

    def upload_working_dir(self, working_root, local_path):
        if urlparse(local_path).scheme in ["s3", "s3n"]:
            return [local_path]
        remote_path = working_root
        return self.s3_upload(local_path, remote_path, recursive=True)

    def clean_working_dir(self, dir_path):
        self.s3_delete_file(dir_path)

    def prepare_working_file(self, working_root, file_path):
        return self.upload_working_file(working_root, file_path)

    def execute_jar(self, job_name, jar_path, jar_args=None, main_class="", *args, **kwargs):
        if not jar_args:
            jar_args = []
        print "EMR Jar job: '%s'" % jar_path
        print "           args = '%s'" % jar_args
        step_ids = self.emr_execute_jar(job_name=job_name,
                                        s3_jar_path=jar_path,
                                        jar_args=jar_args,
                                        main_class=main_class)
        self.emr_describe_steps(step_ids)

        print("Waiting jobflow steps...")
        self._wait_steps(step_ids, **kwargs)
        return step_ids

    def execute_hive(self, job_name, hive_script, *args, **kwargs):
        step_ids = self.emr_execute_hive(job_name, hive_script)
        self.emr_describe_steps(step_ids)

        print("Waiting jobflow steps...")
        self._wait_steps(step_ids, **kwargs)
        return step_ids

    def execute_pig(self, job_name, pig_script, *args, **kwargs):
        step_ids = self.emr_execute_pig(job_name, pig_script)
        self.emr_describe_steps(step_ids)

        print("Waiting jobflow steps...")
        self._wait_steps(step_ids, **kwargs)
        return step_ids

    def emr_execute_jar(self, job_name, s3_jar_path, jar_args, main_class="", action_on_failure='CONTINUE'):
        steps = [JarStep(name=job_name, jar=s3_jar_path,
                         main_class=main_class, step_args=jar_args,
                         action_on_failure=action_on_failure)]
        ret_steps = self.emr_conn.add_jobflow_steps(self.jobflow_id, steps=steps)
        step_ids = [s.value for s in ret_steps.stepids]
        return step_ids

    def emr_execute_hive(self, job_name, s3_hive_script):
        hive_step = HiveStep(name=job_name, hive_file=s3_hive_script)
        hive_step.action_on_failure = 'CONTINUE'
        ret_steps = self.emr_conn.add_jobflow_steps(self.jobflow_id, steps=[hive_step])
        step_ids = [s.value for s in ret_steps.stepids]
        return step_ids

    def emr_execute_pig(self, job_name, s3_pig_script):
        pig_step = PigStep(name=job_name, pig_file=s3_pig_script)
        pig_step.action_on_failure = 'CONTINUE'
        ret_steps = self.emr_conn.add_jobflow_steps(self.jobflow_id, steps=[pig_step])
        step_ids = [s.value for s in ret_steps.stepids]
        return step_ids

    def emr_describe_steps(self, step_ids):
        for sid in step_ids:
            print "==================================="
            print "Summary for step: %s" % sid
            print "==================================="
            step = self.emr_conn.describe_step(self.jobflow_id, sid)
            # print pprint_aws_obj(step)

    def emr_wait_steps(self, stepids):
        for step_id in stepids:
            while True:
                ret = self.emr_conn.describe_step(self.jobflow_id, step_id)
                step_state = ret.status.state
                print "Wait EMR jobflow step: jobflow_id='%s' step_id='%s' state='%s'" % \
                      (self.jobflow_id, step_id, step_state)
                if step_state in ["PENDING", "RUNNING", "CONTINUE"]:
                    time.sleep(10)
                    continue
                elif step_state in ['COMPLETED']:
                    break
                else:
                    return False
        return True

    def s3_upload(self, local_path, remote_s3_path, recursive=False):
        if os.path.isdir(local_path) and not recursive:
            raise ValueError("Can not upload directory, set 'recursive=True'")

        s3_pr = s3parse(remote_s3_path)
        if s3_pr.scheme not in ["s3", "s3n"]:
            raise ValueError("Not valid s3 path: '%s'" % remote_s3_path)
        bucket = self.s3_conn.get_bucket(s3_pr.netloc)

        upload_files = []
        remote_files = []
        if os.path.isfile(local_path):
            upload_files = [local_path]
            remote_files = [s3join(remote_s3_path, os.path.basename(local_path))]
        elif os.path.isdir(local_path):
            upload_files = [os.path.join(dirpath, fn)
                            for dirpath, dirnames, filenames in os.walk(local_path)
                            for fn in filenames]
            remote_files = [s3join(remote_s3_path, fn_local) for fn_local in upload_files]

        for fn_local, fn_remote in izip(upload_files, remote_files):
            s3_upload(bucket, fn_local, fn_remote)

        return remote_files

    def emr_step_log_filename(self, step_id, log_file=None):
        if not log_file:
            log_file = "/"
        return url_path_join(self.emr_info.loguri, self.jobflow_id,
                             "steps", str(self.emr_steps[step_id]["index"]), log_file)

    def emr_step_log(self, step_id, log_file="stdout", local_file=None):
        fn = self.emr_step_log_filename(step_id, log_file)

        pr = s3parse(fn)
        bucket = self.s3_conn.get_bucket(pr.netloc)
        k = boto.s3.key.Key(bucket)
        k.key = pr.path
        if not local_file:
            return k.get_contents_as_string()
        else:
            # TODO: Not implemented
            raise Exception("Not implemented")

    def _wait_steps(self, step_ids, **kwargs):
        log_files = kwargs.get("log_files", ["controller", "stderr", "stdout"])
        retry_count = kwargs.get("retry_count", 10)
        retry_interval = kwargs.get("retry_interval", 60)
        extra_wait_count = kwargs.get("extra_wait_count", 5)
        if not self.emr_wait_steps(step_ids):
            for step_id in step_ids:
                self.dump_logs(step_id, log_files=log_files, retry_count=retry_count,
                               retry_interval=retry_interval,
                               extra_wait_count=extra_wait_count)
            raise Exception("EmrCluster: failed to execute emr_hive steps")


class GenericHadoopCluster(BaseCluster):
    """A Generic Hadoop Cluster."""

    def __init__(self, **cluster_kws):
        super(GenericHadoopCluster, self).__init__()
        self.hs2_host = cluster_kws.get("HiveServer2_Host", None)
        self.hs2_port = cluster_kws.get("HiveServer2_Port", None)
        self.hadoop_conf_dir = self._get_default_hadoop_conf_dir(**cluster_kws)
        self.cluster_env_vars = {}

    def _get_default_hadoop_conf_dir(self, **cluster_kws):
        return cluster_kws.get("default_hadoop_conf_dir", "/etc/hadoop/conf/")

    def prepare(self, hadoop_type, **cluster_kws):
        if hadoop_type not in ["CDH4", "CDH5"]:
            raise Exception("GenericHadoopCluster :: Can NOT prepare '%s'" % hadoop_type)

        if "hadoop_conf" in cluster_kws:
            tmp_hadoop_conf_dir = prepare_hadoop_conf(cluster_kws["hadoop_conf"])
            if not tmp_hadoop_conf_dir:
                default_hadoop_conf_dir = self._get_default_hadoop_conf_dir(**cluster_kws)
                print "Use default hadoop_conf_dir '%s'" % default_hadoop_conf_dir
                self.hadoop_conf_dir = default_hadoop_conf_dir
            else:
                self.hadoop_conf_dir = tmp_hadoop_conf_dir

        env = os.environ.copy()
        cluster_def = cluster_kws.get("cluster_def_path", "/opt/cdh/env.json")
        self.cluster_env_vars = \
            preprocess_cluster_envs(base_envs=env,
                                    hadoop_type=hadoop_type,
                                    cluster_def=cluster_def)
        self.cluster_env_vars["HADOOP_CONF_DIR"] = self.hadoop_conf_dir

    def get_working_root(self, cluster_params, global_params):
        remote_path = "{hdfs_root}/zetjob/{username}/job{job_id}/blk{blk_id}/".format(
            hdfs_root=cluster_params["hdfs_root"],
            username=global_params["userName"],
            job_id=global_params["jobId"],
            blk_id=global_params["blockId"]
        )
        return remote_path

    def hadoop_cmd(self, args, shell=False, verbose=True, extra_env=None):
        if verbose:
            print("Execute External Command : '%s'" % args)
        env = os.environ.copy()
        env.update(self.cluster_env_vars)
        if extra_env:
            env.update(extra_env)

        ret = subprocess.call(args, shell=shell, env=env)
        if verbose:
            print("Exit with exit code = %d" % ret)
        return ret

    def upload_working_file(self, working_root, file_path):
        if self.hadoop_cmd(["hadoop", "fs", "-test", "-d", working_root]):
            self.hadoop_cmd(["hadoop", "fs", "-mkdir", "-p", working_root])

        if not self.hadoop_cmd(["hadoop", "fs", "-copyFromLocal", file_path, working_root]):
            return s3join(working_root, file_path)
        else:
            return None

    def upload_working_dir(self, working_root, local_path):
        if self.hadoop_cmd(["hadoop", "fs", "-test", "-d", working_root]):
            self.hadoop_cmd(["hadoop", "fs", "-mkdir", "-p", working_root])

        if not self.hadoop_cmd(["hadoop", "fs", "-copyFromLocal", local_path, working_root]):
            return s3join(working_root, local_path)
        else:
            return None

    def clean_working_dir(self, dir_path):
        return self.hadoop_cmd(["hadoop", "fs", "-rm", "-r", "-f", dir_path])

    def prepare_working_file(self, working_root, file_path):
        return file_path

    def execute_jar(self, job_name, jar_path, jar_args, main_class, *args, **kwargs):
        this_command = ["hadoop", "jar", jar_path]
        this_command.extend(jar_args)
        return self.hadoop_cmd(this_command)

    def execute_stream_jar(self, jar_file, jar_args, *args, **kwargs):
        pass

    def execute_hive(self, job_name, hive_script, **kwargs):
        hive_db = kwargs.get("db", "default")
        hive_jdbc = "jdbc:hive2://%s:%s/%s" % (self.hs2_host, self.hs2_port, hive_db)
        return self.hadoop_cmd(["beeline", "-u", hive_jdbc,
                                "-n", "hive", "-p", "tiger",
                                "-d", "org.apache.hive.jdbc.HiveDriver",
                                "-f", hive_script, "--verbose=true"])

    def execute_pig(self, job_name, pig_script, *args, **kwargs):
        return self.hadoop_cmd(["pig", "-x", "mapreduce", pig_script])
