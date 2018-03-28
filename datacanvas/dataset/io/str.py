# -*- coding: utf-8 -*-

from .io import Io


class Str(Io):
    def __init__(self, url):
        if url.startswith('str://'):
            self._str = url[6:]
        else:
            raise ValueError('Url is not Str protocol.')

    def read_all(self):
        return self._str
