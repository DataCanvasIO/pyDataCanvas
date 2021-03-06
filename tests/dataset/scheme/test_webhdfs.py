# -*- coding: utf-8 -*-

import os
from getpass import getuser

from datacanvas.dataset import DataSet


def test_webhdfs():
    msg = '{ "hello": "world" }'
    user = getuser()
    url = 'webhdfs://' + user + '@localhost:50070/user/' + user + '/test_output'
    try:
        o = DataSet(url=url, format='text')
        o.put_raw(msg)
        i = DataSet(url=url, fromat='json')
        content_read = i.get_raw()
        assert content_read['hello'] == 'world'
    finally:
        cmd = 'hadoop fs -rm /user/' + user + '/test_output'
        os.system(cmd)
