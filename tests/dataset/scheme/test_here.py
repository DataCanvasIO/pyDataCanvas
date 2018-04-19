# -*- coding: utf-8 -*-

import pytest

from datacanvas.dataset import DataSet


def test_raw_text_here():
    content = 'I am here text'
    url = 'here://' + content
    i = DataSet(url, 'text')
    content_read = i.get_raw()
    assert content_read == content
    o = DataSet(url, 'text')
    with pytest.raises(NotImplementedError):
        o.put_raw(content)


def test_raw_binary_here():
    content = 'I am here text'
    url = 'here://' + content
    with pytest.raises(ValueError):
        DataSet(url)
