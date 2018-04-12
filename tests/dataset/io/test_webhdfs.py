# -*- coding: utf-8 -*-

from getpass import getuser

from datacanvas.dataset import DataSet


def test_webhdfs():
    msg = '{ "hello": "world" }'
    user = getuser()
    url = 'webhdfs://' + user + '@localhost:50070/user/' + user + '/test_output'
    o = DataSet('text:' + url)
    o.write(msg)
    i = DataSet('json:' + url)
    content_read = i.read()
    assert content_read['hello'] == 'world'
