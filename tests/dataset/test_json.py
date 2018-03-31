# -*- coding: utf-8 -*-

import pytest

from datacanvas.dataset import DataSet


def test_json_here():
    url = 'json:here://{ "root": { "leaf1": "1", "leaf2": 2 } }'
    i = DataSet(url)
    content_read = i.read()
    assert content_read['root']['leaf1'] == '1'
    assert content_read['root']['leaf2'] == 2
    o = DataSet(url)
    with pytest.raises(NotImplementedError):
        o.write('')
