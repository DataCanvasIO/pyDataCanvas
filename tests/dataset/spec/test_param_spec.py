# -*- coding: utf-8 -*-

from datacanvas.dataset import DataSet, Spec


def test_param_here():
    url = 'here://{ }'
    spec = Spec.get('param_spec', '{ "var1": { "Type": "string", "Default": "xixihaha" } }')
    i = DataSet(url, 'json', spec)
    content_read = i.get_raw()
    assert content_read.var1 == 'xixihaha'
    url = 'here://{ "var1": "hello" }'
    i = DataSet(url, 'json', spec)
    content_read = i.get_raw()
    assert content_read.var1 == 'hello'
