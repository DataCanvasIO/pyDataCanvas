# -*- coding: utf-8 -*-

"""
A series of Runtime.
"""

import itertools
from datacanvas import EmrCluster
from datacanvas.utils import *
from datacanvas.module import get_settings_from_file


class DatacanvasRuntime(object):
    def __init__(self, spec_filename="spec.json"):
        self.settings = get_settings_from_file(spec_filename)

    def __repr__(self):
        return str(self.settings)


class HadoopRuntime(DatacanvasRuntime):
    def __init__(self, spec_filename="spec.json"):
        super(HadoopRuntime, self).__init__(spec_filename=spec_filename)

    @property
    def hdfs_root(self):
        ps = self.settings
        if 'hdfs_root' in ps.Param._asdict():
            return ps.Param.hdfs_root.val
        else:
            return '/'

    def get_hdfs_working_dir(self, path=""):
        ps = self.settings
        glb_vars = ps.GlobalParam
        remote_path = os.path.normpath(
            os.path.join('tmp/zetjob', glb_vars['userName'], "job%s" % glb_vars['jobId'], "blk%s" % glb_vars['blockId'],
                         path))
        return os.path.join(self.hdfs_root, remote_path)

    def get_hive_namespace(self):
        ps = self.settings
        glb_vars = ps.GlobalParam
        return "zetjobns_%s_job%s_blk%s" % (glb_vars['userName'], glb_vars['jobId'], glb_vars['blockId'])

    def hdfs_upload_dir(self, local_dir):
        for root_dir, dirs, files in os.walk(local_dir):
            for f in sorted(files):
                f = os.path.normpath(os.path.join(root_dir, f))
                f_remote = self.get_hdfs_working_dir(f)
                hdfs_safe_upload(f, f_remote)
                yield f_remote

    def hdfs_clean_working_dir(self):
        hdfs_working_dir = self.get_hdfs_working_dir()
        if not clean_hdfs_path(hdfs_working_dir):
            # TODO : refactor to 'HiveException'
            raise Exception("Can not clean hdfs path : %s" % hdfs_working_dir)

    def clean_working_dir(self):
        self.hdfs_clean_working_dir()


class EmrRuntime(HadoopRuntime):
    def __init__(self, spec_filename="spec.json"):
        import boto
        from boto.emr.connection import EmrConnection, RegionInfo

        super(EmrRuntime, self).__init__(spec_filename)
        p = self.settings.Param
        self.s3_conn = boto.connect_s3(p.AWS_ACCESS_KEY_ID, p.AWS_ACCESS_KEY_SECRET)
        self.s3_bucket = self.s3_conn.get_bucket(p.S3_BUCKET)
        self.region = p.AWS_Region
        self.emr_conn = EmrConnection(p.AWS_ACCESS_KEY_ID, p.AWS_ACCESS_KEY_SECRET,
                                      region=RegionInfo(name=self.region,
                                                        endpoint=self.region + '.elasticmapreduce.amazonaws.com'))
        self.job_flow_id = p.EMR_jobFlowId
        self.cluster = EmrCluster(p.AWS_Region, p.AWS_ACCESS_KEY_ID, p.AWS_ACCESS_KEY_SECRET, p.EMR_jobFlowId)

    def get_s3_working_dir(self, path=""):
        ps = self.settings
        glb_vars = ps.GlobalParam
        remote_path = os.path.normpath(
            os.path.join(self.s3_bucket.name, 'zetjob', glb_vars['userName'], "job%s" % glb_vars['jobId'],
                         "blk%s" % glb_vars['blockId'], path))
        return os.path.join("s3://", remote_path)

    def get_emr_job_name(self):
        ps = self.settings
        glb_vars = ps.GlobalParam
        return os.path.join('zetjob', glb_vars['userName'], "job%s" % glb_vars['jobId'], "blk%s" % glb_vars['blockId'])

    def s3_upload_dir(self, local_dir):
        print("EmrHiveRuntime.s3_uploader()")
        print("s3_upload_dir :::: %s" % local_dir)

        if local_dir == "":
            return

        if not os.path.isdir(local_dir):
            return

        s3_upload_dir = self.get_s3_working_dir(local_dir)
        ext_files = [f for f in sorted(os.listdir(local_dir)) if os.path.isfile(os.path.join(local_dir, f))]
        for f in ext_files:
            f_local = os.path.join(local_dir, f)
            f_remote_full = self.get_s3_working_dir(os.path.join(local_dir, f))

            print("S3 Upload      :: %s ====> %s" % (f_local, s3_upload_dir))
            print("S3 remote_full :: %s" % f_remote_full)
            yield s3_upload(self.s3_bucket, f_local, f_remote_full)

    def s3_clean_working_dir(self):
        s3_working_dir = self.get_s3_working_dir()
        if not s3_delete(self.s3_bucket, s3_working_dir):
            # TODO : refactor to 'HiveException'
            raise Exception("Can not clean s3 path : %s" % s3_working_dir)

    def s3_upload(self, filename):
        from urlparse import urlparse

        parse_ret = urlparse(filename)
        if parse_ret.scheme == '':
            s3_working_dir = self.get_s3_working_dir()
            file_remote = os.path.join(s3_working_dir, os.path.normpath(os.path.basename(filename)))
            file_remote_full = s3_upload(self.s3_bucket, filename, file_remote)
            return file_remote_full
        elif parse_ret.scheme == 's3':
            return filename
        else:
            raise ValueError("Invalid filename to upload to s3: %s" % filename)

    def clean_working_dir(self):
        self.s3_clean_working_dir()


class HiveRuntime(HadoopRuntime):
    def files_uploader(self, local_dir):
        return self.hdfs_upload_dir(local_dir)

    def hive_output_builder(self, output_name, output_obj):
        # TODO: refactor this method
        ps = self.settings
        glb_vars = ps.GlobalParam
        out_type = output_obj.types[0]
        if out_type.startswith("hive.table"):
            return "zetjob_%s_job%s_blk%s_OUTPUT_%s" % (
                glb_vars['userName'], glb_vars['jobId'], glb_vars['blockId'], output_name)
        elif out_type.startswith("hive.hdfs"):
            return self.get_hdfs_working_dir("OUTPUT_%s" % output_name)
        else:
            raise ValueError("Invalid type for hive, type must start with 'hive.table' or 'hive.hdfs'")

    def header_builder(self, hive_ns, uploaded_files, uploaded_jars):
        # Build Output Tables
        for output_name, output_obj in self.settings.Output._asdict().items():
            output_obj.val = self.hive_output_builder(output_name, output_obj)

        return "\n".join(
            itertools.chain(
                ["ADD FILE %s;" % f for f in uploaded_files],
                ["ADD JAR %s;" % f for f in uploaded_jars],
                ["set hivevar:MYNS = %s;" % hive_ns],
                ["set hivevar:PARAM_%s = %s;" % (k, v) for k, v in self.settings.Param._asdict().items()],
                ["set hivevar:INPUT_%s = %s;" % (k, v.val) for k, v in self.settings.Input._asdict().items()],
                ["set hivevar:OUTPUT_%s = %s;" % (k, v.val) for k, v in self.settings.Output._asdict().items()]))

    def clean_working_dir(self):
        self.hdfs_clean_working_dir()

    def generate_script(self, hive_script, target_filename=None):
        hive_ns = self.get_hive_namespace()

        # Upload files and UDF jars
        if 'FILE_DIR' in self.settings.Param._asdict():
            file_dir = self.settings.Param.FILE_DIR
            uploaded_files = self.files_uploader(file_dir.val)
        else:
            uploaded_files = []

        if 'UDF_DIR' in self.settings.Param._asdict():
            jar_dir = self.settings.Param.UDF_DIR
            uploaded_jars = self.files_uploader(jar_dir.val)
        else:
            uploaded_jars = []

        # Build Input, Output and Param
        header = self.header_builder(hive_ns, uploaded_files, uploaded_jars)
        if target_filename:
            import tempfile

            tmp_file = tempfile.NamedTemporaryFile(prefix="hive_generated_", suffix=".hql", delete=False)
            tmp_file.close()
            target_filename = tmp_file.name

        with open(hive_script, "r") as f, open(target_filename, "w+") as out_f:
            out_f.write("--------------------------\n")
            out_f.write("-- Header\n")
            out_f.write("--------------------------\n")
            out_f.write(header)
            out_f.write("\n")
            out_f.write("--------------------------\n")
            out_f.write("-- Main\n")
            out_f.write("--------------------------\n")
            out_f.write("\n")
            out_f.write(f.read())

        return target_filename

    def execute(self, hive_script, generated_hive_script=None):
        self.clean_working_dir()
        generated_hive_script = self.generate_script(hive_script, generated_hive_script)

        if cmd("beeline -u jdbc:hive2://%s:%s -n hive -p tiger -d org.apache.hive.jdbc.HiveDriver -f '%s' --verbose=true "
            % (self.settings.Param.HiveServer2_Host, self.settings.Param.HiveServer2_Port, generated_hive_script)) != 0:
            raise Exception("Failed to execute hive script : %s" % generated_hive_script)


class EmrHiveRuntime(EmrRuntime, HiveRuntime):
    def __init__(self, spec_filename="spec.json"):
        super(EmrHiveRuntime, self).__init__(spec_filename)

    def hive_output_builder(self, output_name, output_obj):
        # TODO : should refactor this function to base class
        ps = self.settings
        glb_vars = ps.GlobalParam
        out_type = output_obj.types[0]
        if out_type.startswith("hive.table"):
            return "zetjob_%s_job%s_blk%s_OUTPUT_%s" % (
                glb_vars['userName'], glb_vars['jobId'], glb_vars['blockId'], output_name)
        elif out_type.startswith("hive.hdfs"):
            return self.get_hdfs_working_dir("OUTPUT_%s" % output_name)
        elif out_type.startswith("hive.s3"):
            return self.get_s3_working_dir("OUTPUT_%s" % output_name)
        else:
            raise ValueError("Invalid type for hive, type must start with 'hive.table' or 'hive.hdfs' or 'hive.s3'")

    def files_uploader(self, local_dir):
        return self.s3_upload_dir(local_dir)

    def emr_execute_hive(self, s3_hive_script):
        from boto.emr.step import HiveStep

        hive_step = HiveStep(name=self.get_emr_job_name(), hive_file=s3_hive_script)
        ret_steps = self.emr_conn.add_jobflow_steps(self.job_flow_id, steps=[hive_step])

        print("Waiting jobflow steps...")
        if not emr_wait_steps(self.emr_conn, self.job_flow_id, ret_steps):
            raise Exception("EmrHiveRuntime : failed to execute steps")
        return [s.value for s in ret_steps.stepids]

    def execute(self, main_hive_script, generated_hive_script=None):
        self.clean_working_dir()
        hive_script_local = self.generate_script(main_hive_script, generated_hive_script)

        s3_working_dir = self.get_s3_working_dir()
        hive_script_remote = os.path.join(s3_working_dir, os.path.basename(hive_script_local))
        hive_script_remote_full = s3_upload(self.s3_bucket, hive_script_local, hive_script_remote)
        print("========= Generated Hive Script =========")
        print(open(hive_script_local).read())
        print("=========================================")
        print("EmrHiveRuntime.execute()")
        return self.emr_execute_hive(hive_script_remote_full)


class EmrJarRuntime(EmrRuntime):
    def __init__(self, spec_filename="spec.json"):
        super(EmrJarRuntime, self).__init__(spec_filename)

    def execute(self, jar_path, args):
        from boto.emr.step import JarStep

        s3_jar_path = s3_upload(self.s3_bucket, jar_path, self.get_s3_working_dir(jar_path))
        print("Uploading jar to s3 : %s -> %s" % (jar_path, s3_jar_path))

        print("Add jobflow step")
        step = JarStep(name=self.get_emr_job_name(), jar=s3_jar_path, step_args=args)
        ret_steps = self.emr_conn.add_jobflow_steps(self.job_flow_id, steps=[step])

        print("Waiting jobflow steps...")
        if not emr_wait_steps(self.emr_conn, self.job_flow_id, ret_steps):
            raise Exception("EmrJarRuntime : failed to execute steps")
        return [s.value for s in ret_steps.stepids]

class PigRuntime(HadoopRuntime):
    def __init__(self, spec_filename="spec.json"):
        super(PigRuntime, self).__init__(spec_filename)

    def files_uploader(self, local_dir):
        return self.hdfs_upload_dir(local_dir)

    def pig_output_builder(self, output_name, output_obj):
        # TODO: refactor this method
        out_type = output_obj.types[0]
        if out_type.startswith("pig.hdfs"):
            return self.get_hdfs_working_dir("OUTPUT_%s" % output_name)
        else:
            raise ValueError("Invalid type for pig, type must start with 'pig.hdfs'")

    def header_builder(self, uploaded_jars):
        # Build Output Tables
        for output_name, output_obj in self.settings.Output._asdict().items():
            output_obj.val = self.pig_output_builder(output_name, output_obj)

        return "\n".join(
            itertools.chain(
                ["%%declare PARAM_%s '%s'" % (k, v) for k, v in self.settings.Param._asdict().items()],
                ["%%declare INPUT_%s '%s'" % (k, v.val) for k, v in self.settings.Input._asdict().items()],
                ["%%declare OUTPUT_%s '%s'" % (k, v.val) for k, v in self.settings.Output._asdict().items()],
                ["REGISTER '%s';" % f for f in uploaded_jars]
            ))

    def generate_script(self, pig_script, target_filename=None):
        if 'UDF_DIR' in self.settings.Param._asdict():
            jar_dir = self.settings.Param.UDF_DIR
            uploaded_jars = self.files_uploader(jar_dir.val)
        else:
            uploaded_jars = []

        # Build Input, Output and Param
        header = self.header_builder(uploaded_jars)
        if target_filename:
            import tempfile

            tmp_file = tempfile.NamedTemporaryFile(prefix="pig_generated_", suffix=".hql", delete=False)
            tmp_file.close()
            target_filename = tmp_file.name

        with open(pig_script, "r") as f, open(target_filename, "w+") as out_f:
            out_f.write("/*************************\n")
            out_f.write(" * Header\n")
            out_f.write(" *************************/\n")
            out_f.write(header)
            out_f.write("\n")
            out_f.write("/*************************\n")
            out_f.write(" * Main\n")
            out_f.write(" *************************/\n")
            out_f.write("\n")
            out_f.write(f.read())
            out_f.write("\n")

        return target_filename

    def generate_pig_conf(self):
        ps = self.settings

        with open("/home/run/pig.properties", "a") as pf:
            pf.write("fs.default.name=%s\n" % ps.Param.hdfs_root)
            pf.write("yarn.resourcemanager.address=%s\n" % ps.Param.yarn_address)
            pf.write("yarn.resourcemanager.scheduler.address=%s\n" % ps.Param.yarn_scheduler_address)
        cmd("cat /home/run/pig.properties")

    def execute(self, pig_script):
        self.clean_working_dir()

        self.generate_pig_conf()
        generated_pig_script = self.generate_script(pig_script)
        print("========= Generated Pig Script =========")
        print(open(generated_pig_script).read())
        print("=========================================")
        print("EmrHiveRuntime.execute()")
        cmd("pig -x mapreduce -P /home/run/pig.properties %s" % generated_pig_script)


class EmrPigRuntime(EmrRuntime, PigRuntime):
    def __init__(self, spec_filename="spec.json"):
        super(EmrPigRuntime, self).__init__(spec_filename)

    def files_uploader(self, local_dir):
        return self.s3_upload_dir(local_dir)

    def pig_output_builder(self, output_name, output_obj):
        # TODO : should refactor this function to base class
        ps = self.settings
        glb_vars = ps.GlobalParam
        out_type = output_obj.types[0]
        if out_type.startswith("pig.hdfs"):
            return self.get_hdfs_working_dir("OUTPUT_%s" % output_name)
        elif out_type.startswith("pig.s3"):
            return self.get_s3_working_dir("OUTPUT_%s" % output_name)
        else:
            raise ValueError("Invalid type for pig, type must start with 'pig.hdfs' or 'pig.s3'")

    def emr_execute_pig(self, pig_filename):
        from boto.emr.step import PigStep

        s3_pig_script = self.s3_upload(pig_filename)
        pig_step = PigStep(name=self.get_emr_job_name(), pig_file=s3_pig_script)
        ret_steps = self.emr_conn.add_jobflow_steps(self.job_flow_id, steps=[pig_step])

        print("Waiting jobflow steps...")
        if not emr_wait_steps(self.emr_conn, self.job_flow_id, ret_steps):
            raise Exception("EmrHiveRuntime : failed to execute steps")
        return [s.value for s in ret_steps.stepids]

    def execute(self, pig_script):
        self.clean_working_dir()

        # TODO: upload S3 additional files

        generated_pig_script = self.generate_script(pig_script)
        print("========= Generated Pig Script =========")
        print(open(generated_pig_script).read())
        print("=========================================")
        print("EmrHiveRuntime.execute()")
        return self.emr_execute_pig(generated_pig_script)

