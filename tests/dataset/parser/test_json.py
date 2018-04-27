# -*- coding: utf-8 -*-

from datacanvas.dataset import DataSet


def test_json_here():
    url = 'here:{ "root": { "leaf1": "1", "leaf2": 2 } }'
    i = DataSet(url=url, format='json')
    content_read = i.get_raw()
    assert content_read['root']['leaf1'] == '1'
    assert content_read['root']['leaf2'] == 2
