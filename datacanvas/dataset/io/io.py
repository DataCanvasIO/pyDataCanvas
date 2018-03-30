# -*- coding: utf-8 -*-

import sys
from importlib import import_module

from ..common import to_class_name


class Io(object):
    @staticmethod
    def get(url):
        protocol_name = 'file'
        index = url.find(':')
        if index >= 0:
            protocol_name = url[0:index]
        package = sys.modules[__name__].__package__
        module = import_module(package + '.' + protocol_name)
        clazz = getattr(module, to_class_name(protocol_name))
        return clazz(url)

    def read(self):
        raise NotImplementedError('Method read_all is not implemented for %s.' % self.__class__.__name__)

    def write(self, content):
        raise NotImplementedError('Method write_all is not implemented for %s.' % self.__class__.__name__)
