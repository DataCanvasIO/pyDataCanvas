# -*- coding: utf-8 -*-

import os
import re
import sys
import json
import types
from collections import namedtuple
from datacanvas.io_types import load_io_obj, BaseIO
from datacanvas.utils import mask_key


class Input(str):
    def __new__(cls, x, _types):
        return str.__new__(cls, x)

    def __init__(self, x, _types):
        super(Input, self).__init__()
        self.x = x
        self._types = _types

    def __repr__(self):
        return str(self.x)

    def __str__(self):
        return str(self.x)

    def as_first_line(self):
        with open(self.x, "r") as f:
            return f.readline().rstrip()

    def as_whole(self):
        with open(self.x, "r") as f:
            return f.read()

    def as_file(self, mode="r"):
        return open(self.x, mode)

    def as_datasource(self, mode="r"):
        ds = json.loads(open(self.x, mode).read())
        return ds

    def as_obj(self):
        ds = load_io_obj(self.x)
        return ds

    def as_raw(self):
        return str(self.x)

    @property
    def val(self):
        """Unboxing Input depends on its types."""
        if "any" in self._types:
            return self.as_raw()
        elif any([re.match(r"raw.*", t) for t in self._types]):
            return self.as_raw()
        else:
            return self.as_obj()

    @property
    def types(self):
        return self._types


class Output(str):
    def __new__(cls, x, _types):
        return str.__new__(cls, x)

    def __init__(self, x, _types):
        super(Output, self).__init__()
        self.x = x
        self._types = _types

    def __repr__(self):
        return str(self.x)

    def __str__(self):
        return str(self.x)

    def as_first_line(self):
        with open(self.x, "r") as f:
            return f.readline().rstrip()

    def as_whole(self):
        with open(self.x, "r") as f:
            return f.read()

    def as_file(self, mode="r"):
        return open(self.x, mode)

    def as_obj(self):
        ds = load_io_obj(self.x)
        return ds

    def as_raw(self):
        return str(self.x)

    @property
    def val(self):
        """Unboxing Input depends on its types."""
        if "any" in self._types:
            return self.as_raw()
        elif any([re.match(r"raw.*", t) for t in self._types]):
            return self.as_raw()
        else:
            return load_io_obj(self.x)

    @val.setter
    def val(self, value):
        with open(self.x, "w+") as f:
            if "any" in self._types or any([re.match(r"raw.*", t) for t in self._types]):
                return

            if isinstance(value, BaseIO):
                f.write(json.dumps(value))
            else:
                f.write(value)

    @property
    def types(self):
        return self._types


class Param(str):
    def __new__(cls, x, typeinfo,scope):
        return str.__new__(cls, x)

    def __init__(self, x, typeinfo ,scope):  # 声明类的2个属性
        super(Param, self).__init__()
        self._x = x
        self._typeinfo = typeinfo
        self._scope = scope

    def __repr__(self):
        if self.is_cluster:
            return self.show()
        else:
            return str(self._x)

    def __str__(self):
        if self.is_cluster:
            return self.show()
        else:
            return self.val#str(self._x)

    def show(self, mask_keys=True):
        def get_safe_cluster_param(cp):
            security_mask_names = ['accessKey', 'accessSecret', 'qubole_api_token', 'subscriptionId']
            if mask_keys and cp['Name'] in security_mask_names and 'Val' in cp:
                cp['Val'] = mask_key(cp['Val'])
                return cp
            return cp

        o = self.val
        if isinstance(o, dict):
            if self.is_cluster:
                cluster_params = o['Parameters']
                o['Parameters'] = [get_safe_cluster_param(cp) for cp in cluster_params]
                return json.dumps(o)
        else:
            return str(self._x)

    @property
    def type(self):
        return self._typeinfo['Type']

    @property
    def is_primitive(self):
        return self._typeinfo['Type'] not in ["cluster"]

    @property
    def is_cluster(self):
        return self._typeinfo['Type'] in ["cluster"]

    @property  #To call a method into attributes
    def val(self):
        type_handler = {
            "string": lambda x: x,
            "float": lambda x: float(x),
            "integer": lambda x: int(x),
            "enum": lambda x: x,
            "cluster": lambda x: json.loads(x),
            "file": read_whole_file,
            "map": lambda x:x,
            "enum2": lambda x:x
        }
        param_type = self._typeinfo['Type']
        if param_type in type_handler:
            return type_handler[param_type](self._x)
        else:
            return self._x


def read_whole_file(filename):
    with open(filename, "r") as f:
        return f.read()


def gettype(name):
    type_map = {
        "string": "str",
        "integer": "int",
        "float": "float",
        "enum": "str",
        "file": "str"
    }
    if name not in type_map:
        raise ValueError(name)
    t = __builtins__.get(type_map[name], types.StringType)
    if isinstance(t, type):
        return t
    raise ValueError(name)


def input_output_builder(spec_input, spec_output):
    params = dict(arg.split("=") for arg in sys.argv[1:])
    if not all(k in params for k in spec_input.keys()):
        raise ValueError("Missing input parameters")
    if not all(k in params for k in spec_output.keys()):
        raise ValueError("Missing output parameters")

    inputSettings = namedtuple('InputSettings', spec_input.keys())
    in_params = {in_k: Input(params[in_k], in_type) for in_k, in_type in spec_input.items()}
    input_settings = inputSettings(**in_params)

    OutputSettings = namedtuple('OutputSettings', spec_output.keys())
    out_params = {out_k: Output(params[out_k], out_type) for out_k, out_type in spec_output.items()}
    output_settings = OutputSettings(**out_params)

    return input_settings, output_settings


def param_builder(spec_param, param_json):
    def get_param(k):
        if k in param_json:
            return param_json[k]['Val']
        else:
            return spec_param[k]['Default']

    ParamSettings = namedtuple('ParamSettings', spec_param.keys())
    param_dict = {k: Param(get_param(k), v,v.get("scope",None)) for k, v in spec_param.items()}
    env_settings = ParamSettings(**param_dict)
    return env_settings


def global_param_builder(param_json):
    return {k: v['Val'] for k, v in param_json.items()}


def get_settings(spec_json):
    moderate_keys = ['Name', 'Param', 'Input', 'Output', 'Cmd', 'Description']
    if not all(k in spec_json for k in moderate_keys):
        raise ValueError("One of param from %s may not exist in 'spec.json'" % str(moderate_keys))

    # TODO: condition for appending 'GlobalParam'
    moderate_keys.append('GlobalParam')
    ModuleSetting = namedtuple('ModuleSetting', moderate_keys)

    # Load parameters
    param_json = get_json_file(os.getenv("ZETRT"))

    param = param_builder(spec_json['Param'], param_json['PARAM'])
    json_input, json_output = input_output_builder(spec_json['Input'], spec_json['Output'])

    # TODO:
    global_param = global_param_builder(param_json['GLOBAL_PARAM'])
    settings = ModuleSetting(Name=spec_json['Name'], Description=spec_json['Description'], Param=param,
                             Input=json_input, Output=json_output, Cmd=spec_json['Cmd'], GlobalParam=global_param)
    return settings


def get_json_file(filename):
    with open(filename, "r") as f:
        return json.load(f)


def get_settings_from_file(filename):
    with open(filename, "r") as f:
        return get_settings(json.load(f))


def get_settings_from_string(spec_json_str):
    print(json.loads(spec_json_str))
    return get_settings(json.loads(spec_json_str))