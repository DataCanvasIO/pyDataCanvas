# -*- coding: utf-8 -*-

from .io import Io


class Here(Io):
    def __init__(self, url):
        if url.startswith('here://'):
            self._str = url[7:]
        else:
            raise ValueError('Url is not "here" protocol.')

    def read_all(self):
        return self._str
