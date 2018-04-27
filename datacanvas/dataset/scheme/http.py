# -*- coding: utf-8 -*-

from urllib.request import urlopen

from .scheme import Scheme
from ..parser import Parser


class Http(Scheme):
    def __init__(self, spec):
        url = spec['url']
        assert url.startswith('http://')
        self.__url = url
        parser = Parser.get(spec['parser'])
        self.__parser = parser

    def read(self):
        url = self.__url
        parser = self.__parser
        content = urlopen(url).read()
        return parser.loads(content)

    def write(self, content):
        url = self.__url
        parser = self.__parser
        content = parser.dumps(content)
        return urlopen(url).write(content)
