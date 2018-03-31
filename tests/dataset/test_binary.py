# -*- coding: utf-8 -*-

from datacanvas.dataset import DataSet


def test_binary_file():
    url = 'binary:file://test_output_binary_file.bin'
    content_write = b'test_binary_file'
    o = DataSet(url)
    o.write(content_write)
    i = DataSet(url)
    content_read = i.read()
    assert content_read == content_write
