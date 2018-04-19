# -*- coding: utf-8 -*-

from urllib.request import urlopen

from .scheme import Scheme


class Http(Scheme):
    def __init__(self, url, fmt):
        assert url.startswith('http://')
        self.__url = url
        self.__fmt = fmt

    def read(self):
        url = self.__url
        fmt = self.__fmt
        content = urlopen(url).read()
        return fmt.loads(content)

    def write(self, content):
        url = self.__url
        fmt = self.__fmt
        content = fmt.dumps(content)
        return urlopen(url).write(content)
