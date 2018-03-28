# -*- coding: utf-8 -*-

from datacanvas.dataset import DataSet


def test_text_file():
    url = 'file://test_output_text_file.bin'
    content_write = 'test_text_file'
    o = DataSet('text', url)
    o.schema().write_all(content_write)
    i = DataSet('text', url)
    content_read = i.schema().read_all()
    assert (content_read == content_write)


def test_text_str():
    url = 'str://just a test'
    i = DataSet('text', url)
    content_read = i.schema().read_all()
    assert (content_read == 'just a test')
