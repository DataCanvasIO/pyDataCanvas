# -*- coding: utf-8 -*-

from .io import Io


class Here(Io):
    def __init__(self, url, schema):
        assert url.startswith('here://')
        if schema.mode == 'b':
            raise ValueError('Only text mode is supported for "%s".' % self.__class__.__name__)
        self.__content = url[7:]
        self.__schema = schema

    def read(self):
        schema = self.__schema
        return schema.loads(self.__content)

    def write(self, content):
        Io.write(self, content)
