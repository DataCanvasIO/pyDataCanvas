# -*- coding: utf-8 -*-

from .io import Io


class File(Io):
    def __init__(self, url, schema):
        if url.startswith('file://'):
            self.__path = url[7:]
        else:
            self.__path = url
        self.__schema = schema

    def read(self):
        path = self.__path
        schema = self.__schema
        mode = 'r' + schema.mode
        result = schema.load(open(path, mode))
        if result == NotImplemented:
            content = open(path, mode).read()
            return schema.loads(content)
        else:
            return result

    def write(self, content):
        path = self.__path
        schema = self.__schema
        mode = 'w' + schema.mode
        result = schema.dump(content, open(path, mode))
        if result == NotImplemented:
            content = schema.dumps(content)
            return open(path, mode).write(content)
        else:
            return result
