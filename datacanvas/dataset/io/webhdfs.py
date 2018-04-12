# -*- coding: utf-8 -*-

from urllib.parse import urlsplit

from hdfs.client import InsecureClient

from .io import Io


class Webhdfs(Io):
    def __init__(self, url, schema):
        urls = urlsplit(url)
        assert urls.scheme == 'webhdfs'
        self.__client = InsecureClient('http://' + urls.netloc, user=urls.username)
        self.__path = urls.path
        self.__schema = schema

    def read(self):
        client = self.__client
        path = self.__path
        schema = self.__schema
        with client.read(path) as f:
            result = schema.load(f)
            if result == NotImplemented:
                content = f.read()
                result = schema.loads(content)
        return result

    def write(self, content):
        client = self.__client
        path = self.__path
        schema = self.__schema
        content = bytes(content, 'utf-8')
        with client.write(path) as f:
            result = schema.dump(content, f)
            if result == NotImplemented:
                content = schema.dumps(content)
                result = f.write(content)
        return result
