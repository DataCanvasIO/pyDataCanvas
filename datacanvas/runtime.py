# -*- coding: utf-8 -*-

from collections import namedtuple

from .dataset import DataSet
from .dataset import Spec


class Runtime(object):
    def __init__(self, spec_file_url):
        module = DataSet(spec_file_url, 'json', Spec.get('module_spec')).get_raw()
        self.__module = module
        self.Inputs = None
        self.Outputs = None
        self.Params = None

    def set_params(self, param_file_url):
        module = self.__module
        self.Params = DataSet(param_file_url, 'json', Spec.get('param_spec', module.Param)).get_raw()

    def set_inputs(self, args):
        module = self.__module
        if not all(k in args for k in module.Input.keys()):
            raise ValueError("Missing input parameters")
        Inputs = namedtuple('Inputs', module.Input.keys())
        d = {}
        for k, v in module.Input.items():
            meta = DataSet(args[k], 'json', Spec.get('meta_spec')).get_raw()
            d[k] = DataSet(meta.url, meta.format)
        self.Inputs = Inputs(**d)

    def set_outputs(self, args):
        module = self.__module
        if not all(k in args for k in module.Output.keys()):
            raise ValueError("Missing output parameters")
        Outputs = namedtuple('Outputs', module.Output.keys())
        d = {}
        for k, v in module.Output.items():
            meta = DataSet(args[k], 'json', Spec.get('meta_spec')).get_raw()
            d[k] = DataSet(meta.url, meta.format)
        self.Outputs = Outputs(**d)
