# -*- coding: utf-8 -*-

"""
A series of Runtime.
"""
import threading
from ftplib import FTP
import functools
import itertools
import hashlib

import time

from datacanvas import io_types
from datacanvas import version as datacanvas_version
from datacanvas.clusters import EmrCluster, QuboleCluster, EhcCluster, GenericHadoopCluster
from datacanvas.module import get_settings_from_file
from datacanvas.utils import *


class BasicRuntime(object):
    def __init__(self, spec_filename="spec.json"):
        self.settings = get_settings_from_file(spec_filename)

    def __repr__(self):
        return str(self.settings)

    @staticmethod
    def cmd(args, shell=False, verbose=True):
        if verbose:
            print("Execute External Command : '%s'" % args)
        ret = subprocess.call(args, shell=shell, env=os.environ.copy())
        if verbose:
            print("Exit with exit code = %d" % ret)
        return ret

    @staticmethod
    def exit(ret_code):
        sys.exit(ret_code)


def HadoopRuntime(*args, **kwargs):
    from warnings import warn
    warn("Use 'GenericHadoopRuntime' class! 'HadoopRuntime' is deprecated.")
    return GenericHadoopRuntime(*args, **kwargs)


class SparkRunTime():
    @staticmethod
    def upload2Hdfs(setting):
        params = setting[1];
        fileName = params.__getattribute__("SparkJar");
        _moudlename = setting.__getattribute__("Name");
        _hdfsUrl = params.__getattribute__("Ftp2hdfs_Url")
        _ftpuserPW = params.__getattribute__("Ftp2Hdfs_UserPass")
        fp = open(fileName, 'rb')
        content = fp.read()
        fp.close()
        m = hashlib.md5(content)
        md5fileName = m.hexdigest()  # md5
        ftp=FTP()
        ftp.set_debuglevel(0)
        ftp.connect(unicode(_hdfsUrl).split(u':')[0].__str__(),unicode(_hdfsUrl).split(u':')[1].__str__())
        ftp.login(unicode(_ftpuserPW).split(u':')[0].__str__(),unicode(_ftpuserPW).split(u':')[1].__str__())
        try:
            ftp.cwd(_moudlename)
        except:
            ftp.mkd(_moudlename)
        try:
            ftp.size(md5fileName)
            print "file has been uploaded,pass it"
        except:
            bufsize = 1024                           #set buffer
            file_handler = open(fileName,'rb')      #read binary
            ftp.storbinary('STOR %s' % os.path.basename(md5fileName),file_handler,bufsize)    #upload file
            ftp.set_debuglevel(1)
            file_handler.close()
        finally:
            ftp.quit()
        return md5fileName


    @staticmethod
    def notifyParam(setting,fileName):
        params = setting[1];
        _SparkJar = fileName
        _MainClass = params.__getattribute__("MainClass")
        _serverURL = params.__getattribute__("JobProxy_Url")
        _jvmParam = setting[1].__getattribute__("Program_param").val
        _moudleName = setting.__getattribute__("Name")
        _username = unicode(params.__getattribute__("Ftp2Hdfs_UserPass")).split(u':')[0].__str__()
        _type = params.__getattribute__("Type").val

        paramjson = {}
        paramjson["userName"] = _username
        paramjson["type"] = _type
        _hdfsurl = '/user/datacanvas/%s/%s/%s' % (_username,_moudleName,_SparkJar)
        paramjson["data"] = {}
        paramjson["data"]["jar"] = _hdfsurl
        paramjson["data"]["class"] = _MainClass
        paramjson["data"]["args"] = str(_jvmParam).split(",")

        url = 'http://%s/aisle/spark' % _serverURL
        payload = json.dumps(paramjson)
        print payload
        response = requests.request("POST", url, data=payload, headers=default_headers)
        print("spark runtime INFO : notify server %s" %url)
        if(response.status_code == 200):
            res = json.loads(response.text)
            return res["result"]["applicationId"]
        print("spark runtime INFO : server response %s" %(response.text))
        return None

    # 任务完成且成功返回1，完成但失败返回0，未完成返回-1
    @staticmethod
    def getStatus(params,appid):
        _serverURL = params.__getattribute__("JobProxy_Url")
        url = 'http://%s/aisle/spark/%s' % (_serverURL ,appid)
        response = requests.request("GET", url, headers=default_headers)
        res = json.loads(response.text)
        if(res['result']['app']['state'] == 'FINISHED'):
            if(res['result']['app']['finalStatus'] == 'SUCCEEDED'):
                return 1
            return 0
        if(res['result']['app']['state'] == 'FAILED'):
            return 0
        print "spark runtime INFO : job's status is running"
        return -1



    @staticmethod
    def getLog(appid,offset,step,params):
        _serverURL = params.__getattribute__("JobProxy_Url")
        url = 'http://%s/aisle/spark/log/%s?offset=%s&step=%s' % (_serverURL,appid,offset,step)
        response = requests.request("GET", url, headers=default_headers)
        res = json.loads(response.text)
        content = res["result"]["content"]
        print content
        return res["result"]["total_size"]


class EmrRuntime(BasicRuntime):
    def __init__(self, spec_filename="spec.json"):
        super(EmrRuntime, self).__init__(spec_filename)
        self.grt = GenericHadoopRuntime()

    def get_s3_working_dir(self, path=""):
        return self.grt.get_working_dir(path)

    def get_emr_job_name(self):
        return self.grt.get_job_name()


class HiveRuntime(BasicRuntime):
    def __init__(self, spec_filename="spec.json"):
        super(HiveRuntime, self).__init__(spec_filename)
        self.grt = GenericHadoopRuntime()

    def execute(self, hive_script, generated_hive_script=None):
        return self.grt.execute_hive(hive_script)


class EmrHiveRuntime(BasicRuntime):
    def __init__(self, spec_filename="spec.json"):
        super(EmrHiveRuntime, self).__init__(spec_filename)
        self.grt = GenericHadoopRuntime()

    def execute(self, main_hive_script, generated_hive_script=None, dump_logfiles=None, dump_logfile_retry_count=1):
        return self.grt.execute_hive(main_hive_script,
                                     logfiles=dump_logfiles,
                                     retry_count=dump_logfile_retry_count)


class PigRuntime(BasicRuntime):
    def __init__(self, spec_filename="spec.json"):
        super(PigRuntime, self).__init__(spec_filename)
        self.grt = GenericHadoopRuntime()

    def execute(self, pig_script):
        self.grt.execute_pig(pig_script)


class EmrPigRuntime(BasicRuntime):
    def __init__(self, spec_filename="spec.json"):
        super(EmrPigRuntime, self).__init__(spec_filename)
        self.grt = GenericHadoopRuntime()

    def execute(self, pig_script, dump_logfiles=None, dump_logfile_retry_count=1):
        return self.grt.execute_pig(pig_script,
                                    logfiles=dump_logfiles,
                                    retry_count=dump_logfile_retry_count)


class ScriptBuilder(object):

    def __init__(self, settings, s3_working_root, hdfs_working_root, cluster_params=None):
        self.settings = settings
        self.s3_working_root = s3_working_root
        self.hdfs_working_root = hdfs_working_root
        if not cluster_params:
            cluster_params = {}
        self.cluster_params = cluster_params

    def get_hdfs_working_dir(self, dir_path=""):
        return s3join(self.hdfs_working_root, dir_path)

    def get_s3_working_dir(self, dir_path=""):
        return s3join(self.s3_working_root, dir_path)


class HiveScriptBuilder(ScriptBuilder):

    def __init__(self, settings, s3_working_root, hdfs_working_root, cluster_params, ignore_output_type_error=True):
        super(HiveScriptBuilder, self).__init__(settings, s3_working_root, hdfs_working_root, cluster_params)
        self.ignore_output_type_error = ignore_output_type_error

    def get_hive_namespace(self):
        ps = self.settings
        glb_vars = ps.GlobalParam
        return "zetjobns_{userName}_job{job_id}_blk{blk_id}".format(
            userName=glb_vars['userName'],
            job_id=glb_vars['jobId'],
            blk_id=glb_vars['blockId'])

    def get_hive_table(self, output_name):
        ps = self.settings
        glb_vars = ps.GlobalParam
        return "zetjob_{userName}_job{job_id}_blk{blk_id}_OUTPUT_{output_name}".format(
            userName=glb_vars['userName'],
            job_id=glb_vars['jobId'],
            blk_id=glb_vars['blockId'],
            output_name=output_name)

    def hive_output_builder(self, output_name, output_obj):
        out_type = output_obj.types[0]
        # TODO: refactor types
        if out_type.startswith("hive.table"):
            return self.get_hive_table(output_name)
        elif out_type.startswith("hdfs"):
            return self.get_hdfs_working_dir("OUTPUT_%s" % output_name)
        elif out_type.startswith("s3"):
            return self.get_s3_working_dir("OUTPUT_%s" % output_name)
        else:
            raise ValueError("Invalid type for hive")

    def hive_create_output(self, output_name, output_obj):
        cluster_params = self.cluster_params

        out_type = output_obj.types[0]
        if io_types.is_type_of("s3", out_type):
            aws_key = cluster_params.get("accessKey", "")
            aws_sec = cluster_params.get("accessSecret", "")
            return io_types.DS_S3(URL=self.get_s3_working_dir("OUTPUT_%s" % output_name),
                                  aws_key=aws_key,
                                  aws_security=aws_sec)
        elif io_types.is_type_of("hdfs", out_type) or io_types.is_type_of("datasource.hdfs", out_type):
            return io_types.DS_HDFS(URL=self.get_hdfs_working_dir("OUTPUT_%s" % output_name))
        elif io_types.is_type_of("hive", out_type) or io_types.is_type_of("datasource.hive", out_type):
            hs2_host = cluster_params.get("hive_server2_host", "")
            hs2_port = cluster_params.get("hive_server2_port", "")
            if hs2_host and hs2_port:
                return io_types.DS_Hive(URL=self.get_hive_table("OUTPUT_%s" % output_name),
                                        meta_server=hs2_host,
                                        meta_port=hs2_port)
            else:
                return io_types.DS_Hive(URL=self.get_hive_table("OUTPUT_%s" % output_name))
        elif io_types.is_type_of("datasource", out_type):
            # TODO: default as a 'hive' table?
            return io_types.DS_Hive(URL=self.get_hive_table("OUTPUT_%s" % output_name))
        else:
            if self.ignore_output_type_error:
                return None
            else:
                raise ValueError("Invalid type for hive")

    def header_builder(self, hive_ns, uploaded_files, uploaded_jars, extra_vars=None):
        if not extra_vars:
            extra_vars = {}

        # Build Output Tables
        for output_name, output_obj in self.settings.Output._asdict().items():
            output_obj.val = self.hive_create_output(output_name, output_obj)

        def _get_io_val(obj):
            if 'URL' in obj:
                return obj['URL']
            else:
                # TODO:
                if self.ignore_output_type_error:
                    return ""
                else:
                    raise Exception("TODO: ERROR")

        return "\n".join(
            itertools.chain(
                ["ADD FILE %s;" % f for f in uploaded_files],
                ["ADD JAR %s;" % f for f in uploaded_jars],
                ["set hivevar:MYNS = %s;" % hive_ns],
                ["set hivevar:PARAM_%s = %s;" % (k, v) for k, v in self.settings.Param._asdict().items() if v.is_primitive],
                ["set hivevar:INPUT_%s = %s;" % (k, _get_io_val(v.val)) for k, v in self.settings.Input._asdict().items()],
                ["set hivevar:OUTPUT_%s = %s;" % (k, _get_io_val(v.val)) for k, v in self.settings.Output._asdict().items() if v.val],
                ["set hivevar:%s = %s;" % (k, v) for k, v in extra_vars.items()]))

    def generate_script(self, hive_script, uploaded_files, uploaded_jars, extra_vars=None):
        hive_ns = self.get_hive_namespace()

        # Build Input, Output and Param
        header = self.header_builder(hive_ns, uploaded_files, uploaded_jars, extra_vars)

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


class PigScriptBuilder(ScriptBuilder):

    def __init__(self, settings, s3_working_root, hdfs_working_root, cluster_params):
        super(PigScriptBuilder, self).__init__(settings, s3_working_root, hdfs_working_root, cluster_params)

    def pig_output_builder(self, output_name, output_obj):
        out_type = output_obj.types[0]
        # TODO: refactor types
        if out_type.startswith("hdfs"):
            return self.get_hdfs_working_dir("OUTPUT_%s" % output_name)
        elif out_type.startswith("s3"):
            return self.get_s3_working_dir("OUTPUT_%s" % output_name)
        else:
            raise ValueError("Invalid type for hive, type must start with 'hive.table' or 'hive.hdfs' or 'hive.s3'")

    def pig_create_output(self, output_name, output_obj):
        cluster_params = self.cluster_params

        out_type = output_obj.types[0]
        if io_types.is_type_of("s3", out_type):
            aws_key = cluster_params.get("accessKey", None)
            aws_sec = cluster_params.get("accessSecret", None)
            return io_types.DS_S3(URL=self.get_s3_working_dir("OUTPUT_%s" % output_name),
                                  aws_key=aws_key,
                                  aws_security=aws_sec)
        elif io_types.is_type_of("hdfs", out_type):
            return io_types.DS_HDFS(URL=self.get_hdfs_working_dir("OUTPUT_%s" % output_name))
        else:
            raise ValueError("Invalid type for hive")

    def header_builder(self, uploaded_jars, extra_vars=None):
        if not extra_vars:
            extra_vars = {}

        # Build Output Tables
        for output_name, output_obj in self.settings.Output._asdict().items():
            output_obj.val = self.pig_create_output(output_name, output_obj)

        return "\n".join(
            itertools.chain(
                ["%%declare PARAM_%s '%s'" % (k, v)
                 for k, v in self.settings.Param._asdict().items()
                 if v.is_primitive],
                ["%%declare INPUT_%s '%s'" % (k, v.val)
                 for k, v in self.settings.Input._asdict().items()],
                ["%%declare OUTPUT_%s '%s'" % (k, v.val)
                 for k, v in self.settings.Output._asdict().items()],
                ["%%declare %s '%s'" % (k, v.val)
                 for k, v in extra_vars.items()],
                ["REGISTER '%s';" % f
                 for f in uploaded_jars]
            ))

    def generate_script(self, pig_script, uploaded_jars, extra_vars=None):

        # Build Input, Output and Param
        header = self.header_builder(uploaded_jars, extra_vars)

        import tempfile
        tmp_file = tempfile.NamedTemporaryFile(prefix="pig_generated_", suffix=".pig", delete=False)
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




class GenericHadoopRuntime(BasicRuntime):

    def __init__(self, cluster_var_name="cluster"):
        super(GenericHadoopRuntime, self).__init__()
        self.hadoop_type = None
        self.cluster = None
        self.working_root = None
        self.hdfs_working_root = None
        self.s3_working_root = None
        self.global_params = self.settings.GlobalParam
        self.cluster_params = None

        param_dict = self.settings.Param._asdict()
        if (cluster_var_name in param_dict) and param_dict[cluster_var_name].is_cluster:
            cluster_type = self._get_cluster_type(cluster_var_name)
            print param_dict.get(cluster_var_name).type
            print "=================================================="
            print "Use cluster var :: '%s'" % cluster_var_name
            print "           type :: '%s'" % cluster_type
            print "=================================================="
            self.switch_hadoop_env(cluster_type, cluster_var_name)

    def _get_cluster_type(self, cluster_var_name):
        cparam = self.settings.Param._asdict().get(cluster_var_name).val
        return cparam["Type"]

    def _get_cluster_params(self, cluster_var_name):
        cparam = self.settings.Param._asdict().get(cluster_var_name).val
        cluster_params = {p["Name"]: p.get("Val", None) for p in cparam["Parameters"]}
        return cluster_params

    def switch_hadoop_env(self, hadoop_type, cluster_var_name="cluster", extra_env_vars=None):
        print "Switching to Hadoop type = '%s'" % hadoop_type
        print "pyDataCanvas version : '%s'" % datacanvas_version
        cluster_params = self._get_cluster_params(cluster_var_name)
        self.cluster_params = cluster_params

        if hadoop_type in ["EMR", "EMR_SPOT"]:
            self.hadoop_type = hadoop_type
            self.cluster = EmrCluster(aws_region=cluster_params["region"],
                                      aws_key=cluster_params["accessKey"],
                                      aws_secret=cluster_params["accessSecret"],
                                      jobflow_id=cluster_params["jobFlowId"])
            self.cluster.prepare(hadoop_type, **cluster_params)
            self.working_root = self.cluster.get_working_root(cluster_params, self.global_params)
            self.s3_working_root = self.working_root
            self.hdfs_working_root = "/"
        elif hadoop_type in ["CDH4", "CDH5"]:
            self.hadoop_type = hadoop_type
            self.cluster = GenericHadoopCluster(**cluster_params)
            self.cluster.prepare(hadoop_type, **cluster_params)
            self.working_root = self.cluster.get_working_root(cluster_params, self.global_params)
            self.s3_working_root = None
            self.hdfs_working_root = cluster_params["hdfs_root"]
        elif hadoop_type in ["QUBOLE"]:
            self.hadoop_type = hadoop_type
            self.cluster = QuboleCluster(qubole_api_token=cluster_params["qubole_api_token"],
                                         qubole_cluster_name=cluster_params["qubole_cluster_name"],
                                         qubole_tags=cluster_params["qubole_tags"])
            self.cluster.prepare(hadoop_type, **cluster_params)
            self.working_root = self.cluster.get_working_root(cluster_params, self.global_params)
            self.s3_working_root = self.working_root
            self.hdfs_working_root = "/"
        elif hadoop_type in ["EHC"]:
            self.hadoop_type = hadoop_type
            self.cluster = EhcCluster(ehc_token=cluster_params["ehc_token"],
                                      ehc_id=cluster_params["ehc_id"])
            self.cluster.prepare(hadoop_type, **cluster_params)
            self.working_root = self.cluster.get_working_root(cluster_params, self.global_params)
            self.s3_working_root = self.working_root
            self.hdfs_working_root = "/"
        else:
            raise Exception("Do NOT support hadoop_type '%s'" % hadoop_type)

        self.cluster.clean_working_dir(self.working_root)
        print self.working_root

    def get_working_dir(self, path=""):
        if not self.working_root:
            raise ValueError("Did NOT define 'working_root'!")

        return s3join(self.working_root, path)

    def get_job_name(self):
        ps = self.settings
        glb_vars = ps.GlobalParam
        return os.path.join('zetjob', glb_vars['userName'],
                            "job%s" % glb_vars['jobId'], "blk%s" % glb_vars['blockId'])

    def execute_jar(self, jar_path, jar_args, main_class="", *args, **kwargs):
        job_name = self.get_job_name()

        remote_jar_path = self.cluster.prepare_working_file(self.working_root, jar_path)
        return self.cluster.execute_jar(job_name=job_name, jar_path=remote_jar_path,
                                        jar_args=jar_args, main_class=main_class,
                                        *args, **kwargs)

    def execute_hive_filename(self, hive_main_filename, extra_vars=None, *args, **kwargs):
        job_name = self.get_job_name()

        hb = HiveScriptBuilder(self.settings, s3_working_root=self.s3_working_root,
                               hdfs_working_root=self.hdfs_working_root,
                               cluster_params=self.cluster_params)
        generated_hql = hb.generate_script(hive_main_filename, [], [], extra_vars=extra_vars)
        remote_hive_script = self.cluster.prepare_working_file(self.working_root, generated_hql)
        ret = self.cluster.execute_hive(job_name=job_name,
                                        hive_script=remote_hive_script,
                                        *args, **kwargs)
        return ret

    def execute_pig_filename(self, pig_main_filename, extra_vars=None, *args, **kwargs):
        job_name = self.get_job_name()

        pb = PigScriptBuilder(self.settings, s3_working_root=self.s3_working_root,
                              hdfs_working_root=self.hdfs_working_root,
                              cluster_params=self.cluster_params)
        generated_pig = pb.generate_script(pig_main_filename, [], extra_vars=extra_vars)
        remote_pig_script = self.cluster.prepare_working_file(self.working_root, generated_pig)
        ret = self.cluster.execute_pig(job_name=job_name,
                                       pig_script=remote_pig_script,
                                       *args, **kwargs)
        return ret


class SparkThread(threading.Thread):
    def run(self):
        for i in range(3):
            time.sleep(1)
            msg = "I'm "+self.name+' @ '+str(i)
            print msg
##############
# Decorators #
##############

class DataCanvas(object):
    """DataCanvas"""

    def __init__(self, name):
        self._name = name
        self._graph = []
        self._rt = None

    def basic_runtime(self, spec_json="spec.json"):
        def decorator(method):
            rt = BasicRuntime(spec_filename=spec_json)
            params = rt.settings.Param
            inputs = rt.settings.Input
            outputs = rt.settings.Output

            @functools.wraps(method)
            def wrapper(_rt=rt, _params=params, _inputs=inputs, _outputs=outputs):
                print rt
                method(_rt, _params, _inputs, _outputs)

            self._graph.append(wrapper)
            return wrapper

        return decorator

    def spark_jar_runtime(self,spec_json="spec.json"):
        def decorator(method):
            rt = BasicRuntime(spec_filename=spec_json)
            params = rt.settings.Param
            _type = params.__getattribute__("Type")
            inputs = rt.settings.Input
            outputs = rt.settings.Output

            @functools.wraps(method)
            def wrapper(_rt=rt,_params=params,_inputs=inputs,_outputs=outputs):
                fileName = SparkRunTime().upload2Hdfs(rt.settings)
                appid = SparkRunTime().notifyParam(rt.settings,fileName)
                threading.Thread(target = method, args = (_rt, _params, _inputs, _outputs)).start()
                offset = 0
                step=10240
                total_size = 1
                if(_type=="spark"):
                    while 1:
                        code = SparkRunTime().getStatus(rt.settings[1],appid)
                        if(code == 1):
                            print "spark runtime INFO : job [%s] success" % appid
                            if(offset < total_size):
                                total_size = SparkRunTime().getLog(appid,offset,step,rt.settings[1])
                                offset = offset + step
                            break
                        if(code == 0):
                            print "spark runtime INFO : job [%s] failure" % appid
                            if(offset < total_size):
                                total_size = SparkRunTime().getLog(appid,offset,step,rt.settings[1])
                                offset = offset + step
                            else:
                                break
                        if(code == -1):
                            time.sleep(5)
                    print rt
                elif(_type == "spark_r"):
                    _attempt = 0
                    while 1:
                        total_size = SparkRunTime().getLog(appid,offset,step,rt.settings[1])
                        offset = offset + step
                        if(total_size == -2 and _attempt<5):
                            _attempt += 1
                            print ("spark runtime INFO : attempt %s,log is being prepared" % _attempt)
                        if (total_size != -1 and total_size != -2 and offset >= total_size):
                            break
                        time.sleep(5)

            self._graph.append(wrapper)
            return wrapper

        return decorator;

    def hadoop_runtime(self, spec_json="spec.json"):
        def decorator(method):
            rt = GenericHadoopRuntime(cluster_var_name="cluster")
            params = rt.settings.Param
            inputs = rt.settings.Input
            outputs = rt.settings.Output

            @functools.wraps(method)
            def wrapper(_rt=rt, _params=params, _inputs=inputs, _outputs=outputs):
                print rt
                method(_rt, _params, _inputs, _outputs)

            self._graph.append(wrapper)
            return wrapper

        return decorator

    def run(self):
        for m in self._graph:
            m()
