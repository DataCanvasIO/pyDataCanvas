# -*- coding: utf-8 -*-

from datacanvas.dataset import DataSet


def test_binary_file():
    url = 'file://test_output_binary_file.bin'
    content_write = 'test_binary_file'
    o = DataSet('binary', url)
    o.schema().write_all(content_write)
    i = DataSet('binary', url)
    content_read = i.schema().read_all()
    assert content_read == content_write
