# -*- coding: utf-8 -*-

from datacanvas.dataset import DataSet, Spec


def test_param_here():
    url = 'json:here://{ }'
    spec = Spec.get('param_spec', '{ "var1": { "Type": "string", "Default": "xixihaha" } }')
    i = DataSet(url, spec)
    content_read = i.read()
    assert content_read.var1 == 'xixihaha'
    url = 'json:here://{ "var1": "hello" }'
    i = DataSet(url, spec)
    content_read = i.read()
    assert content_read.var1 == 'hello'
