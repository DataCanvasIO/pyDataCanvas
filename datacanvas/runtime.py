# -*- coding: utf-8 -*-

"""
A series of Runtime.
"""

from __future__ import print_function

from datacanvas.module import get_settings_from_file
from datacanvas.utils import *


class Runtime(object):
    def __init__(self, spec_filename="spec.json"):
        self.settings = get_settings_from_file(spec_filename)

    def __repr__(self):
        return str(self.settings)

    @staticmethod
    def cmd(args, shell=False, verbose=True):
        if verbose:
            print("Execute External Command : '%s'" % args)
        ret = subprocess.call(args, shell=shell, env=os.environ.copy())
        if verbose:
            print("Exit with exit code = %d" % ret)
        return ret

    @staticmethod
    def exit(ret_code):
        sys.exit(ret_code)
