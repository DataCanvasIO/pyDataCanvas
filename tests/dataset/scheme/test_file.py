# -*- coding: utf-8 -*-

from datacanvas.dataset import DataSet


def test_text_file():
    url = 'file://test_output_text_file.bin'
    content_write = 'test_text_file'
    o = DataSet(url=url, format='text')
    o.put_raw(content_write)
    i = DataSet(url=url, format='text')
    content_read = i.get_raw()
    assert content_read == content_write


def test_binary_file():
    url = 'file://test_output_binary_file.bin'
    content_write = b'test_binary_file'
    o = DataSet(url=url, format='binary')
    o.put_raw(content_write)
    i = DataSet(url=url, format='binary')
    content_read = i.get_raw()
    assert content_read == content_write
