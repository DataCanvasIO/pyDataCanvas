# -*- coding: utf-8 -*-

from collections import namedtuple

from .dataset import DataSet, ModuleSpec, Params


class Runtime(object):
    def __init__(self, spec_file_url):
        module = ModuleSpec(spec_file_url).get_module()
        self.__module = module
        self.Inputs = None
        self.Outputs = None
        self.Params = None

    def set_params(self, param_file_url):
        module = self.__module
        self.Params = Params(param_file_url, module.Param).get_params()

    def set_inputs(self, args):
        module = self.__module
        if not all(k in args for k in module.Input.keys()):
            raise ValueError("Missing input parameters")
        Inputs = namedtuple('Inputs', module.Input.keys())
        d = {}
        for k, v in module.Input.items():
            d[k] = DataSet(args[k])
        self.Inputs = Inputs(**d)

    def set_outputs(self, args):
        module = self.__module
        if not all(k in args for k in module.Output.keys()):
            raise ValueError("Missing output parameters")
        Outputs = namedtuple('Outputs', module.Output.keys())
        d = {}
        for k, v in module.Output.items():
            d[k] = DataSet(args[k])
        self.Outputs = Outputs(**d)
