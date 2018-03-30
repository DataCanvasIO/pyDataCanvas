# -*- coding: utf-8 -*-


def to_class_name(s):
    return ''.join(x.capitalize() for x in s.split('_'))
