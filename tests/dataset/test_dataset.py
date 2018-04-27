# -*- coding: utf-8 -*-

import pytest

from datacanvas.dataset import DataSet


def test_json_str_param():
    d = DataSet('{"url": "here:", "format": "text"}')
    spec = d.spec()
    assert spec['url'] == 'here:'
    assert spec['format'] == 'text'


def test_dict_param():
    d = DataSet({'url': 'here:', 'format': 'text'})
    spec = d.spec()
    assert spec['url'] == 'here:'
    assert spec['format'] == 'text'


def test_keyword_parm():
    d = DataSet(url='here:', format='text')
    spec = d.spec()
    assert spec['url'] == 'here:'
    assert spec['format'] == 'text'


def test_json_spec_file_parm():
    d = DataSet('here:{"url": "here:", "format": "text"}')
    spec = d.spec()
    assert spec['url'] == 'here:'
    assert spec['format'] == 'text'


def test_spec_missing_url():
    with pytest.raises(ValueError):
        d = DataSet(format='text')


def test_spec_missing_format():
    with pytest.raises(ValueError):
        d = DataSet(url="here:")
