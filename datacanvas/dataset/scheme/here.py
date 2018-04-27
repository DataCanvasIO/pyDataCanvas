# -*- coding: utf-8 -*-

from .scheme import Scheme
from ..parser import Parser


class Here(Scheme):
    def __init__(self, spec):
        url = spec['url']
        assert url.startswith('here:')
        if 'data' not in spec:
            self.__content = url[5:]
        else:
            self.__content = spec['data']
        parser = Parser.get(spec['parser'])
        if parser.mode == 'b':
            raise ValueError('Only text mode is supported for "%s".' % self.__class__.__name__)
        self.__parser = parser

    def read(self):
        parser = self.__parser
        return parser.loads(self.__content)

    def write(self, content):
        Scheme.write(self, content)
