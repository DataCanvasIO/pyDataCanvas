# -*- coding: utf-8 -*-

import os

from livy import LivySession


my_dir =  os.path.dirname(__file__)


def main():
    code = open(my_dir+'/run.py').read()
    code += '\n\nmain(sc)'
    with LivySession('http://localhost:8998') as session:
        print(code)
        o = session.run(code)
        print(o)


def test_spark():
    main()
