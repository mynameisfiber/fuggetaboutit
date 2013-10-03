#!/usr/bin/env python

import contextlib
import time

@contextlib.contextmanager
def TimingBlock(name, N=None):
    start = time.time()
    yield
    dt = time.time() - start
    if N:
        print "[%0.1f][timing] %s: %fs (%f / s with %d trials)" % (start, name, dt, N/dt, N)
    else:
        print "[%0.1f][timing] %s: %fs" % (start, name, dt)

