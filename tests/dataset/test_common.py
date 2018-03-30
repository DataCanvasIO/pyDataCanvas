# -*- coding: utf-8 -*-

from datacanvas.dataset.common import to_class_name


def test_to_class_name():
    assert to_class_name('param_spec') == 'ParamSpec'
