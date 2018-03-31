# -*- coding: utf-8 -*-

from datacanvas.dataset import DataSet


def test_text_file():
    url = 'text:file://test_output_text_file.bin'
    content_write = 'test_text_file'
    o = DataSet(url)
    o.write(content_write)
    i = DataSet(url)
    content_read = i.read()
    assert content_read == content_write


def test_text_here():
    url = 'text:here://just a test'
    i = DataSet(url)
    content_read = i.read()
    assert content_read == 'just a test'
