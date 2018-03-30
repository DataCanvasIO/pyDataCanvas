# -*- coding: utf-8 -*-

from importlib import import_module


def to_class_name(s):
    return ''.join(x.capitalize() for x in s.split('_'))


def get_module_class(module_name, package=None):
    module = import_module('.' + module_name, package)
    return getattr(module, to_class_name(module_name))
