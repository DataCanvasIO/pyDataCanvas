# -*- coding: utf-8 -*-

from urllib.parse import urlsplit

from hdfs.client import InsecureClient

from .scheme import Scheme
from ..parser import Parser


class Webhdfs(Scheme):
    def __init__(self, spec):
        url = spec['url']
        urls = urlsplit(url)
        assert urls.scheme == 'webhdfs'
        if 'user' in spec:
            user = spec['user']
        else:
            user = urls.username
        self.__client = InsecureClient('http://' + urls.netloc, user=user)
        self.__path = urls.path
        parser = Parser.get(spec['parser'])
        self.__parser = parser

    def read(self):
        client = self.__client
        path = self.__path
        parser = self.__parser
        with client.read(path) as f:
            result = parser.load(f)
            if result == NotImplemented:
                content = f.read()
                result = parser.loads(content)
        return result

    def write(self, content):
        client = self.__client
        path = self.__path
        parser = self.__parser
        content = bytes(content, 'utf-8')
        with client.write(path) as f:
            result = parser.dump(content, f)
            if result == NotImplemented:
                content = parser.dumps(content)
                result = f.write(content)
        return result
