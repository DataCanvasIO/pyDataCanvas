# -*- coding: utf-8 -*-

import sys
from importlib import import_module


class Io(object):
    @staticmethod
    def get(url):
        protocol_name = 'file'
        index = url.find(':')
        if index >= 0:
            protocol_name = url[0:index]
        package = sys.modules[__name__].__package__
        module = import_module(package + '.' + protocol_name)
        clazz = getattr(module, protocol_name.capitalize())
        return clazz(url)
