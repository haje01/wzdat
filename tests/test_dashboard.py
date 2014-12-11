import os
import urllib2
from subprocess import Popen

import pytest

from wzdat.dashboard.app import app
from wzdat.make_config import make_config

cfg = make_config()


def test_home():
    #req = urllib2.Request('http://localhost:8085')
    #f = urllib2.urlopen(req)
    #print f.read()
    pass
