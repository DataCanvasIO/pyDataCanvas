# -*- coding: utf-8 -*-

from datacanvas.dataset import Input, Output


def test_binary_file():
    url = 'file://test_output_binary_file.bin'
    content_write = 'test_binary_file'
    o = Output('binary', url)
    o.schema().write_all(content_write)
    i = Input('binary', url)
    content_read = i.schema().read_all()
    assert (content_read == content_write)


if __name__ == '__main__':
    test_binary_file()
