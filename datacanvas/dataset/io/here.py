# -*- coding: utf-8 -*-

from .io import Io


class Here(Io):
    def __init__(self, url):
        if url.startswith('here://'):
            self.__content = url[7:]
        else:
            raise ValueError('Url is not "here" protocol.')

    def read(self):
        return self.__content

    def write(self, content):
        Io.write(self, content)
