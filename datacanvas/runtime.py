# -*- coding: utf-8 -*-

from collections import namedtuple

from .dataset import DataSet
from .dataset import Spec


class Runtime(object):
    def __init__(self, spec_file_url):
        module = DataSet(
            schema_name='json',
            url=spec_file_url,
            spec=Spec.get('module_spec')
        ).read()
        self.__module = module
        self.Inputs = None
        self.Outputs = None
        self.Params = None

    def set_params(self, param_file_url):
        module = self.__module
        self.Params = DataSet(
            schema_name='json',
            url=param_file_url,
            spec=Spec.get('param_spec', module.Param),
        ).read()

    def set_inputs(self, args):
        module = self.__module
        if not all(k in args for k in module.Input.keys()):
            raise ValueError("Missing input parameters")
        Inputs = namedtuple('Inputs', module.Input.keys())
        for k, v in module.Input.items():
            print(k)
            print(v)
        self.Inputs = Inputs(**{k: DataSet(
            schema_name=v['schema'],
            url=args[k],
            spec=v['spec'],
        ) for k, v in module.Input.items()})

    def set_outputs(self, args):
        module = self.__module
        if not all(k in args for k in module.Output.keys()):
            raise ValueError("Missing output parameters")
        Outputs = namedtuple('Outputs', module.Output.keys())
        self.Outputs = Outputs(**{k: DataSet(
            schema_name=v['schema'],
            url=args[k],
            spec=v['spec'],
        ) for k, v in module.Output.items()})
