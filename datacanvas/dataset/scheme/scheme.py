# -*- coding: utf-8 -*-

from ..common import get_module_class


class Scheme(object):
    @staticmethod
    def get(spec):
        url = spec['url']
        scheme_name = 'file'
        index = url.find(':')
        if index >= 0:
            scheme_name = url[0:index]
        clazz = get_module_class(scheme_name, __name__)
        return clazz(spec)

    def read(self):
        raise NotImplementedError('Method read is not implemented for "%s".' % self.__class__.__name__)

    def write(self, content):
        raise NotImplementedError('Method write is not implemented for "%s".' % self.__class__.__name__)
