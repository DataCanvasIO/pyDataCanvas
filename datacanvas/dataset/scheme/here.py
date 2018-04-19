# -*- coding: utf-8 -*-

from .scheme import Scheme


class Here(Scheme):
    def __init__(self, url, fmt):
        assert url.startswith('here://')
        if fmt.mode == 'b':
            raise ValueError('Only text mode is supported for "%s".' % self.__class__.__name__)
        self.__content = url[7:]
        self.__fmt = fmt

    def read(self):
        fmt = self.__fmt
        return fmt.loads(self.__content)

    def write(self, content):
        Scheme.write(self, content)
