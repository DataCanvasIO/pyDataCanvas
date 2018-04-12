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
        with open(path, mode) as f:
            result = schema.load(f)
            if result == NotImplemented:
                content = f.read()
                result = schema.loads(content)
        return result

    def write(self, content):
        path = self.__path
        schema = self.__schema
        mode = 'w' + schema.mode
        with open(path, mode) as f:
            result = schema.dump(content, f)
            if result == NotImplemented:
                content = schema.dumps(content)
                result = f.write(content)
        return result
