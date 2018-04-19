# -*- coding: utf-8 -*-

from ..common import get_module_class
from ..fmt import Fmt


class Scheme(object):
    @staticmethod
    def get(url, fmt):
        scheme = 'file'
        index = url.find(':')
        if index >= 0:
            scheme = url[0:index]
        clazz = get_module_class(scheme, __name__)
        fmt_obj = Fmt.get(fmt)
        return clazz(url, fmt_obj)

    def read(self):
        raise NotImplementedError('Method read is not implemented for "%s".' % self.__class__.__name__)

    def write(self, content):
        raise NotImplementedError('Method write is not implemented for "%s".' % self.__class__.__name__)
