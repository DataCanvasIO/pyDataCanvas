# -*- coding: utf-8 -*-

from getpass import getuser

from datacanvas.dataset import DataSet


def test_webhdfs():
    msg = '{ "hello": "world" }'
    user = getuser()
    url = 'webhdfs://' + user + '@localhost:50070/user/' + user + '/test_output'
    o = DataSet(url, 'text')
    o.write(msg)
    i = DataSet(url, 'json')
    content_read = i.get_raw()
    assert content_read['hello'] == 'world'
