# -*- coding: utf-8 -*-

import pytest

from datacanvas.dataset.common import get_module_class
from datacanvas.dataset.common import to_class_name


def test_to_class_name():
    assert to_class_name('param_spec') == 'ParamSpec'


def test_get_module_class():
    with pytest.raises(ValueError):
        get_module_class('test_common', __name__)
