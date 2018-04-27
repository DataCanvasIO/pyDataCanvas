# -*- coding: utf-8 -*-

from .scheme import Scheme
from ..parser import Parser


class File(Scheme):
    def __init__(self, spec):
        url = spec['url']
        if url.startswith('file://'):
            self.__path = url[7:]
        elif url.startswith('file:'):
            self.__path = url[5:]
        else:
            self.__path = url
        parser = Parser.get(spec['parser'])
        self.__parser = parser

    def read(self):
        path = self.__path
        parser = self.__parser
        mode = 'r' + parser.mode
        with open(path, mode) as f:
            result = parser.load(f)
            if result is NotImplemented:
                content = f.read()
                result = parser.loads(content)
        return result

    def write(self, content):
        path = self.__path
        parser = self.__parser
        mode = 'w' + parser.mode
        with open(path, mode) as f:
            result = parser.dump(content, f)
            if result is NotImplemented:
                content = parser.dumps(content)
                result = f.write(content)
        return result
