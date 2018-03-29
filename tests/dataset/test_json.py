# -*- coding: utf-8 -*-

from datacanvas.dataset import DataSet


def test_json_here():
    url = 'here://{ "root": { "leaf1": "1", "leaf2": 2 } }'
    i = DataSet('json', url)
    content_read = i.schema().read_all()
    assert content_read['root']['leaf1'] == '1'
    assert content_read['root']['leaf2'] == 2
