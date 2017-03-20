#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

import time


from datacanvas.module import Param

from datacanvas.new_runtime import DataCanvas
dc = DataCanvas(__name__)


@dc.spark_jar_runtime(spec_json="spec2.json")
def my_module(rt, params, inputs, outputs):
    # TODO : Fill your code here
    print "Done"
    print "Done===========》"
    print params



# json数组转换成dict
if __name__ == "__main__":
    dc.run()

# 1. 用screwjack提交
# 2. spec.json的param中要用本文件中的spec.json格式
# 3. main.py中注解改变
# 例子：20170303ljspark9