#!/usr/bin/env python2.7

import sys
import os
dev_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, dev_path)

os.environ['DEBUG'] = '1'

import mr.app.main

mr.app.main.app.run()
