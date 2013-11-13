#!/usr/bin/env python

from fuggetaboutit.timing_bloom_filter import TimingBloomFilter
import tornado.ioloop
import tornado.testing
import time
from fuggetaboutit.utils import TimingBlock, TestFile

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


    def test_optimization_size(self):
        tbf = TimingBloomFilter(500, decay_time=4, ioloop=self.io_loop)
        assert len(tbf.data) < tbf.num_bytes
        assert tbf._ENTRIES_PER_8BYTE == 2

    def test_save(self):
        tbf = TimingBloomFilter(5, decay_time=30, ioloop=self.io_loop).start()
        tbf += "hello"

        assert "hello" in tbf
        prev_num_nonzero = tbf.num_non_zero

        tbf.tofile(open("test.tbf", "w+"))

        with TestFile("test.tbf") as fd:
            tbf2 = TimingBloomFilter.fromfile(fd)
        assert "hello" in tbf
        assert prev_num_nonzero == tbf2.num_non_zero


    def test_holistic(self):
        n = int(2e4)
        N = int(1e4)
        T = 3
        print "TimingBloom with capacity %e and expiration time %ds" % (n, T)

        with TimingBlock("Initialization"):
            tbf = TimingBloomFilter(n, decay_time=T, ioloop=self.io_loop)

        orig_decay = tbf.decay
        def new_decay(*args, **kwargs):
            with TimingBlock("Decaying"):
                val = orig_decay(*args, **kwargs)
            return val
        setattr(tbf, "decay", new_decay)
        tbf._setup_decay()
        tbf.start()

        print "num_hashes = %d, num_bytes = %d" % (tbf.num_hashes, tbf.num_bytes)
        print "sizeof(TimingBloom) = %d bytes" % (tbf.num_bytes)

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
            t = T - (time.time() - last_insert) + 1
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

        assert tbf.num_non_zero == 0, "All entries in the bloom should be zero: %d non-zero entries" % tbf.num_non_zero
