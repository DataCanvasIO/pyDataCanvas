# -*- coding: utf-8 -*-

from ..common import get_module_class
from ..schema import Schema


class Io(object):
    @staticmethod
    def get(url):
        index = url.find(':')
        if index < 0:
            raise ValueError('Url must specify a schema.')
        schema_name = url[0:index]
        schema = Schema.get(schema_name)
        url = url[index + 1:]
        protocol_name = 'file'
        index = url.find(':')
        if index >= 0:
            protocol_name = url[0:index]
        clazz = get_module_class(protocol_name, __name__)
        return clazz(url, schema)

    def read(self):
        raise NotImplementedError('Method read is not implemented for "%s".' % self.__class__.__name__)

    def write(self, content):
        raise NotImplementedError('Method write is not implemented for "%s".' % self.__class__.__name__)
