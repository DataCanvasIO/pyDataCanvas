# -*- coding: utf-8 -*-

from .scheme import Scheme


class DataSet(object):
    def __init__(self, url, fmt, spec=None):
        self.__url = url
        self.__fmt = fmt
        self.__spec = spec

    def __read(self, fmt):
        url = self.__url
        scheme = Scheme.get(url, fmt)
        spec = self.__spec
        content = scheme.read()
        if spec:
            content = spec.input(content)
        return content

    def __write(self, content, fmt):
        url = self.__url
        scheme = Scheme.get(url, fmt)
        spec = self.__spec
        if spec:
            content = spec.output(content)
        return scheme.write(content)

    def get_raw(self):
        fmt = self.__fmt
        return self.__read('raw.' + fmt)

    def get_dataframe(self, engine='pandas'):
        fmt = self.__fmt
        return self.__read(engine + '.' + fmt)

    def put_raw(self, content):
        fmt = self.__fmt
        return self.__write(content, 'raw.' + fmt)

    def put_dataframe(self, content, engine='pandas'):
        fmt = self.__fmt
        return self.__write(content, engine + '.' + fmt)
