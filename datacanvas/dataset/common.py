# -*- coding: utf-8 -*-

import sys
from importlib import import_module


def to_class_name(s):
    return ''.join(x.capitalize() for x in s.split('_'))


def get_module_class(module_name, self_module_name=''):
    paths = module_name.split('.')
    if self_module_name.split('.')[-1] == paths[0]:
        raise ValueError('Cannot import %s itself.' % self_module_name)
    package = None
    if self_module_name:
        package = sys.modules[self_module_name].__package__
    module = import_module('.' + module_name, package)
    return getattr(module, to_class_name(paths[-1]))
