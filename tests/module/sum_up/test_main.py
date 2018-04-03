# -*- coding: utf-8 -*-

import os

from datacanvas import DataCanvas
from datacanvas.dataset import DataSet

dc = DataCanvas(__name__)

my_dir = 'file://' + os.path.dirname(__file__)
output_result_url = 'text:' + my_dir + '/test_output_result.txt'


@dc.runtime(
    spec_file_url='json:' + my_dir + '/spec.json',
    param_file_url='json:' + my_dir + '/param.json',
    args={'array': 'json:here://[2, 3, 7, 5, 8]', 'result': output_result_url},
)
def main(runtime, params, inputs, outputs):
    from .run import main
    main(params, inputs, outputs)


def test_main():
    dc.run()
    result = DataSet(output_result_url).read()
    assert result == '25'
