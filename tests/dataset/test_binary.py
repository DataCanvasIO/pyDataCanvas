# -*- coding: utf-8 -*-

from datacanvas.dataset import DataSet


def test_binary_file():
    url = 'file://test_output_binary_file.bin'
    content_write = 'test_binary_file'
    o = DataSet('binary', url)
    o.write(content_write)
    i = DataSet('binary', url)
    content_read = i.read()
    assert content_read == content_write
