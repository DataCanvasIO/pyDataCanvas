# -*- coding: utf-8 -*-

import os
from datacanvas import DataCanvas

dc = DataCanvas(__name__)


@dc.basic_runtime(spec_json='spec.json')
def main(rt, params, inputs, outputs):
    from .run import main
    main(params, inputs, outputs)


def test_main():
    os.environ
    dc.run()


if __name__ == '__main__':
    test_main()
