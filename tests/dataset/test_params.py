# -*- coding: utf-8 -*-

from datacanvas.dataset import Params


def test_param_here():
    url = 'here:{ }'
    spec = {'var1': {'Type': 'string', 'Default': 'xixihaha'}}
    i = Params(url, spec)
    content_read = i.get_params()
    assert content_read.var1 == 'xixihaha'
    url = 'here:{ "var1": "hello" }'
    i = Params(url, spec)
    content_read = i.get_params()
    assert content_read.var1 == 'hello'
