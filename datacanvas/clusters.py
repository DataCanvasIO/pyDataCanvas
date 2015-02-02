# -*- coding: utf-8 -*-

import os
import time
import boto
import boto.s3
import boto.s3.key
import boto.emr
from boto.emr.step import HiveStep, JarStep, PigStep
from itertools import izip
from datacanvas.utils import cached_property, url_path_join, s3join
from datacanvas.utils import s3parse, s3_upload, pprint_aws_obj


class EmrCluster(object):
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

    def emr_execute_jar(self, job_name, s3_jar, args, main_class="", action_on_failure='CONTINUE'):
        steps = [JarStep(name=job_name, jar=s3_jar,
                         main_class=main_class,  step_args=args,
                         action_on_failure=action_on_failure)]
        ret_steps = self.emr_conn.add_jobflow_steps(self.jobflow_id, steps=steps)
        step_ids = [s.value for s in ret_steps.stepids]
        return step_ids

    def emr_execute_hive(self, job_name, s3_hive_scripts):
        hive_step = [HiveStep(name=job_name, hive_file=hs) for hs in s3_hive_scripts]
        ret_steps = self.emr_conn.add_jobflow_steps(self.jobflow_id, steps=[hive_step])
        step_ids = [s.value for s in ret_steps.stepids]
        return step_ids

    def emr_execute_pig(self, job_name, s3_pig_scripts):
        pig_step = [PigStep(name=job_name, pig_file=ps) for ps in s3_pig_scripts]
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
        # TODO: handle this
        # if urlparse(local_filename).scheme in ["s3", "s3n"]:
        #      return local_filename

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

