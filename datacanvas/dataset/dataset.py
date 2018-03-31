# -*- coding: utf-8 -*-

from .io import Io


class DataSet(object):
    def __init__(self, url, spec=None):
        self.__io = Io.get(url)
        self.__spec = spec

    def read(self):
        io = self.__io
        spec = self.__spec
        content = io.read()
        if spec:
            content = spec.input(content)
        return content

    def write(self, content):
        io = self.__io
        spec = self.__spec
        if spec:
            content = spec.output(content)
        return io.write(content)
