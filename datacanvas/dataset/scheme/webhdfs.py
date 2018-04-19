# -*- coding: utf-8 -*-

from urllib.parse import urlsplit

from hdfs.client import InsecureClient

from .scheme import Scheme


class Webhdfs(Scheme):
    def __init__(self, url, fmt):
        urls = urlsplit(url)
        assert urls.scheme == 'webhdfs'
        self.__client = InsecureClient('http://' + urls.netloc, user=urls.username)
        self.__path = urls.path
        self.__fmt = fmt

    def read(self):
        client = self.__client
        path = self.__path
        fmt = self.__fmt
        with client.read(path) as f:
            result = fmt.load(f)
            if result == NotImplemented:
                content = f.read()
                result = fmt.loads(content)
        return result

    def write(self, content):
        client = self.__client
        path = self.__path
        fmt = self.__fmt
        content = bytes(content, 'utf-8')
        with client.write(path) as f:
            result = fmt.dump(content, f)
            if result == NotImplemented:
                content = fmt.dumps(content)
                result = f.write(content)
        return result
