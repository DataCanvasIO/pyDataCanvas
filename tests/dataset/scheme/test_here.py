# -*- coding: utf-8 -*-

import pytest

from datacanvas.dataset import DataSet


def test_text_here():
    content = 'I am here text'
    url = 'here:' + content
    i = DataSet(url=url, format='text')
    content_read = i.get_raw()
    assert content_read == content
    o = DataSet(url=url, format='text')
    with pytest.raises(NotImplementedError):
        o.put_raw(content)


def test_binary_here():
    content = 'I am here text'
    url = 'here:' + content
    i = DataSet(url=url, format='binary')
    with pytest.raises(ValueError):
        i.get_raw()


def test_with_data_spec():
    content = 'I am here text in data'
    i = DataSet(url='here:', format='text', data=content)
    content_read = i.get_raw()
    assert content_read == content
