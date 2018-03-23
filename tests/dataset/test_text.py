# -*- coding: utf-8 -*-

from datacanvas.dataset import Input, Output


def test_text_file():
    url = 'file://test_output_text_file.bin'
    content_write = 'test_text_file'
    o = Output('text', url)
    o.schema().write_all(content_write)
    i = Input('text', url)
    content_read = i.schema().read_all()
    assert (content_read == content_write)


if __name__ == '__main__':
    test_text_file()
