# -*- coding: utf-8 -*-

from .io import Io


class Here(Io):
    def __init__(self, url, schema):
        if schema.mode == 'b':
            raise ValueError('Only text mode is supported for "%s".' % self.__class__.__name__)
        if url.startswith('here://'):
            self.__content = url[7:]
        else:
            raise ValueError('Url is not "here" protocol.')
        self.__schema = schema

    def read(self):
        schema = self.__schema
        return schema.loads(self.__content)

    def write(self, content):
        Io.write(self, content)
