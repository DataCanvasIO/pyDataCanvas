# -*- coding: utf-8 -*-

import os

from datacanvas import DataCanvas
from datacanvas.dataset import DataSet

dc = DataCanvas(__name__)

output_url = 'file://test_output_result.txt'


@dc.runtime(
    spec_file_url='file://' + os.path.dirname(__file__) + '/spec.json',
    param_file_url='file://' + os.path.dirname(__file__) + '/param.json',
    args={'array': 'here://[2, 3, 7, 5, 8]', 'result': output_url},
)
def main(runtime, params, inputs, outputs):
    from .run import main
    main(params, inputs, outputs)


def test_main():
    dc.run()
    result = DataSet('text', output_url)
    assert result.read() == '25'
