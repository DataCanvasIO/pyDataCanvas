# -*- coding: utf-8 -*-

from .io import Io


class File(Io):
    def __init__(self, url):
        if url.startswith('file://'):
            self._path = url[7:]
        else:
            self._path = url

    def read_all(self):
        path = self._path
        with open(path, 'r') as f:
            return f.read()

    def write_all(self, content):
        path = self._path
        with open(path, 'w') as f:
            return f.write(content)

    def open(self, flags):
        path = self._path
        return open(path, flags)
