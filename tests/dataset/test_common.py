# -*- coding: utf-8 -*-

import pytest

from datacanvas.dataset.common import get_module_class
from datacanvas.dataset.common import to_class_name


def test_to_class_name():
    assert to_class_name('param_spec') == 'ParamSpec'


def test_get_module_class():
    clazz = get_module_class('dir1.dir2.for_test_common', __name__)
    obj = clazz()
    assert isinstance(obj, clazz)

    with pytest.raises(ValueError):
        get_module_class('test_common', __name__)
