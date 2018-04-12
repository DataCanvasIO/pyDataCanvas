# -*- coding: utf-8 -*-

from urllib.request import urlopen
from .io import Io


class Http(Io):
    def __init__(self, url, schema):
        assert url.startswith('http://')
        self.__url = url
        self.__schema = schema

    def read(self):
        url = self.__url
        schema = self.__schema
        content = urlopen(url).read()
        return schema.loads(content)

    def write(self, content):
        url = self.__url
        schema = self.__schema
        content = schema.dumps(content)
        return urlopen(url).write(content)
