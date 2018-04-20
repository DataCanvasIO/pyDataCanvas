# -*- coding: utf-8 -*-

from .scheme import Scheme


class File(Scheme):
    def __init__(self, url, fmt):
        if url.startswith('file://'):
            self.__path = url[7:]
        else:
            self.__path = url
        self.__fmt = fmt

    def read(self):
        path = self.__path
        fmt = self.__fmt
        mode = 'r' + fmt.mode
        with open(path, mode) as f:
            result = fmt.load(f)
            if result is NotImplemented:
                content = f.read()
                result = fmt.loads(content)
        return result

    def write(self, content):
        path = self.__path
        fmt = self.__fmt
        mode = 'w' + fmt.mode
        with open(path, mode) as f:
            result = fmt.dump(content, f)
            if result is NotImplemented:
                content = fmt.dumps(content)
                result = f.write(content)
        return result
