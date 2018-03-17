# encoding:utf-8
from __future__ import print_function
from builtins import object
import base64
import subprocess
import os
import json
import requests
from datacanvas.utils import FileUtil

_home = os.path.expandvars('$HOME')


class colors(object):
    BLACK = '\033[0;30m'
    DARK_GRAY = '\033[1;30m'
    LIGHT_GRAY = '\033[0;37m'
    BLUE = '\033[0;34m'
    LIGHT_BLUE = '\033[1;34m'
    GREEN = '\033[0;32m'
    LIGHT_GREEN = '\033[1;32m'
    CYAN = '\033[0;36m'
    LIGHT_CYAN = '\033[1;36m'
    RED = '\033[0;31m'
    LIGHT_RED = '\033[1;31m'
    PURPLE = '\033[0;35m'
    LIGHT_PURPLE = '\033[1;35m'
    BROWN = '\033[0;33m'
    YELLOW = '\033[1;33m'
    WHITE = '\033[1;37m'
    DEFAULT_COLOR = '\033[00m'
    RED_BOLD = '\033[01;31m'
    ENDC = '\033[0m'


class Validator(object):
    @staticmethod
    def validate():
        _kbConfig = json.loads(os.environ.get("kerberosConfig"))
        _kdcLocation = _kbConfig["kdcLocation"]
        _kServer = _kbConfig["kServer"]
        _userName = _kbConfig["kUserName"]
        _userRealm = _kbConfig["realm"]
        _keyTabServer = _kbConfig["keyTabServer"]
        _keyTabPort = _kbConfig["keyTabPort"]
        # 1.  下载keyTab文件到~/.kb dir
        kbDir = FileUtil.createDir(os.path.join(_home, ".kb"))
        url = "http://%s:%s/v1/keytab/retrieve" % (_keyTabServer, _keyTabPort)
        reqHeader = {
            'content-type': "application/json",
            'cache-control': "no-cache",
            '_username': _userName.__str__()
        }
        _keyTabFile = os.path.join(kbDir, "tmp.keyTab")
        r = requests.request("GET", url, headers=reqHeader)
        print('[INFO]: keytab =====> %s' % base64.b64encode(r.content))
        with open(_keyTabFile, "wb") as code:
            code.write(r.content)

        with open("/etc/krb5.conf", 'r') as f:
            krb5File = f.read().__str__()

        str = '''[realms]
%s = {
    kdc = %s
    admin_server = %s
}''' % (_userRealm, _kdcLocation, _kServer)

        if krb5File.find(str) == -1:
            newKrb5File = "%s\n%s" % (krb5File, str)
            with open('/etc/krb5.conf', 'w') as f:
                f.write(newKrb5File)
            print("write file success")
        else:
            print("not need write config")

        kInitCommand = "kinit -kt %s %s" % (_keyTabFile.__str__(), _userName.__str__())
        print("[Info]: %s" % kInitCommand)
        res = subprocess.call(kInitCommand, shell=True)
        if not res == 0:
            print("[Error]: module cannot run, authentication first\n")
            exit(-1)
        print("[Info]: now is klist output:")
        subprocess.call("klist")
        print("[Info]: authentication success, now start run module")
