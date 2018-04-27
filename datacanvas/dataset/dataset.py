# -*- coding: utf-8 -*-

import json

from .scheme import Scheme


class DataSet(object):
    def __init__(self, *args, **kwargs):
        spec = {}
        if len(args) > 0:
            spec_def = args[0]
            if isinstance(spec_def, str):
                if spec_def.lstrip().startswith('{'):
                    spec = json.loads(spec_def)
                else:
                    spec = DataSet(url=spec_def, format='json').get_raw()
            elif isinstance(spec_def, dict):
                spec = spec_def
            else:
                raise ValueError("Spec of DataSet is not valid!")
        if len(kwargs) > 0:
            for (k, v) in kwargs.items():
                spec[k] = v
        spec['parser'] = spec['format']
        self.__spec = spec
        self.__check()

    def __check(self):
        spec = self.__spec
        required_keys = ['url', 'format']
        if not all(k in spec for k in required_keys):
            raise ValueError('Spec of a DataSet must have "url" and "format" keys!')

    def url(self):
        return self.__spec['url']

    def fmt(self):
        return self.__spec['format']

    def spec(self):
        return self.__spec

    def set_parser(self, parser):
        self.__spec['parser'] = parser

    def get_raw(self):
        scheme = Scheme.get(self.__spec)
        return scheme.read()

    def put_raw(self, content):
        scheme = Scheme.get(self.__spec)
        return scheme.write(content)
