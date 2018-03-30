# -*- coding: utf-8 -*-

from .io import Io


class File(Io):
    def __init__(self, url):
        if url.startswith('file://'):
            self.__path = url[7:]
        else:
            self.__path = url

    def read(self):
        path = self.__path
        with open(path, 'r') as f:
            return f.read()

    def write(self, content):
        path = self.__path
        with open(path, 'w') as f:
            return f.write(content)
