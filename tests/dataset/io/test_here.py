# -*- coding: utf-8 -*-

import pytest

from datacanvas.dataset import DataSet


def test_text_here():
    content = 'I am here text'
    url = 'text:here://' + content
    i = DataSet(url)
    content_read = i.read()
    assert content_read == content
    o = DataSet(url)
    with pytest.raises(NotImplementedError):
        o.write(content)


def test_binary_here():
    content = 'I am here text'
    url = 'binary:here://' + content
    with pytest.raises(ValueError):
        DataSet(url)
