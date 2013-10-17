#!/usr/bin/env python

import contextlib
import time
import os

@contextlib.contextmanager
def TimingBlock(name, N=None):
    start = time.time()
    yield
    dt = time.time() - start
    if N:
        print "[%0.1f][timing] %s: %fs (%f / s with %d trials)" % (start, name, dt, N/dt, N)
    else:
        print "[%0.1f][timing] %s: %fs" % (start, name, dt)

@contextlib.contextmanager
def TestFile(filename, mode='r'):
    fd = open(filename, mode)
    yield fd
    fd.close()
    os.unlink(filename)

