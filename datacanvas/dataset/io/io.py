# -*- coding: utf-8 -*-

import sys

from ..common import get_module_class


class Io(object):
    @staticmethod
    def get(url):
        protocol_name = 'file'
        index = url.find(':')
        if index >= 0:
            protocol_name = url[0:index]
        clazz = get_module_class(protocol_name, sys.modules[__name__].__package__)
        return clazz(url)

    def read(self):
        raise NotImplementedError('Method read_all is not implemented for %s.' % self.__class__.__name__)

    def write(self, content):
        raise NotImplementedError('Method write_all is not implemented for %s.' % self.__class__.__name__)
