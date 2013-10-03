#!/usr/bin/env python

from timing_bloom_filter import TimingBloomFilter
import tornado.ioloop
import tornado.testing
import struct
import time
from utils import TimingBlock

class TestTimingBloomFilter(tornado.testing.AsyncTestCase):
    def test_decay(self):
        tbf = TimingBloomFilter(500, decay_time=4, ioloop=self.io_loop).start()
        tbf += "hello"
        assert tbf.contains("hello") == True
        try:
            self.wait(timeout = 4)
        except:
            pass
        assert tbf.contains("hello") == False

    def test_holistic(self):
        n = int(2e4)
        N = int(1e4)
        T = 5
        print "TimingBloom with capacity %e and expiration time %ds" % (n, T)

        with TimingBlock("Initialization"):
            tbf = TimingBloomFilter(n, decay_time=T, dtype="B", ioloop=self.io_loop)

        orig_decay = tbf.decay
        def new_decay(*args, **kwargs):
            with TimingBlock("Decaying"):
                val = orig_decay(*args, **kwargs)
            return val
        setattr(tbf, "decay", new_decay)
        tbf._setup_decay()
        tbf.start()

        print "num_hashes = %d, num_bytes = %d" % (tbf.num_hashes, tbf.num_bytes)
        print "sizeof(TimingBloom) = %d bytes" % (struct.calcsize(tbf.dtype) * tbf.num_bytes)

        with TimingBlock("Adding %d values" % N, N):
            for i in xrange(N):
                tbf.add(str(i))
        last_insert = time.time()

        with TimingBlock("Testing %d positive values" % N, N):
            for i in xrange(N):
                assert str(i) in tbf

        with TimingBlock("Testing %d negative values" % N, N):
            err = 0
            for i in xrange(N, 2*N):
                if str(i) in tbf:
                    err += 1
            tot_err = err / float(N)
            assert tot_err <= tbf.error, "Error is too high: %f > %f" % (tot_err, tbf.error)

        try:
            t = T - (time.time() - last_insert)
            if t > 0:
                self.wait(timeout = t)
        except:
            pass

        with TimingBlock("Testing %d expired values" % N, N):
            err = 0
            for i in xrange(N):
                if str(i) in tbf:
                    err += 1
            tot_err = err / float(N)
            assert tot_err <= tbf.error, "Error is too high: %f > %f" % (tot_err, tbf.error)
