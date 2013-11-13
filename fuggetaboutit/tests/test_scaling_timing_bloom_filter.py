#!/usr/bin/env python

from fuggetaboutit.scaling_timing_bloom_filter import ScalingTimingBloomFilter
import tornado.ioloop
import tornado.testing
import time
from fuggetaboutit.utils import TimingBlock, TestFile

class TestScalingTimingBloomFilter(tornado.testing.AsyncTestCase):
    def test_decay(self):
        stbf = ScalingTimingBloomFilter(500, decay_time=4, ioloop=self.io_loop).start()
        stbf += "hello"
        assert stbf.contains("hello") == True
        try:
            self.wait(timeout = 4)
        except:
            pass
        assert stbf.contains("hello") == False


    def test_save(self):
        stbf = ScalingTimingBloomFilter(5, decay_time=30, ioloop=self.io_loop).start()
        stbf += "hello"

        assert "hello" in stbf
        prev_num_nonzero = stbf.blooms[0]['bloom'].num_non_zero
        stbf.tofile(open("test.stbf", "w+"))

        with TestFile("test.stbf") as fd:
            stbf2 = ScalingTimingBloomFilter.fromfile(fd)

        assert "hello" in stbf
        assert prev_num_nonzero == stbf2.blooms[0]['bloom'].num_non_zero


    def test_size_stability(self):
        stbf = ScalingTimingBloomFilter(10, decay_time=5, min_fill_factor=0.2, growth_factor=2, ioloop=self.io_loop).start()
        for i in xrange(100):
            stbf.add("FOO%d" % i)

        assert len(stbf.blooms) > 0, "Did not scale up"

        for i in xrange(100, 130):
            stbf.add("FOO%d" % i)
            try:
                self.wait(timeout = .5)
            except:
                pass
            if len(stbf.blooms) == 1 and stbf.blooms[0]['id'] == 1:
                return
        assert "Did not scale down"


    def test_holistic(self):
        n = int(1e4)
        N = int(2e4)
        T = 3
        print "ScalingTimingBloom with capacity %e and expiration time %ds" % (n, T)

        with TimingBlock("Initialization"):
            stbf = ScalingTimingBloomFilter(n, decay_time=T, ioloop=self.io_loop)

        orig_decay = stbf.decay
        def new_decay(*args, **kwargs):
            with TimingBlock("Decaying"):
                val = orig_decay(*args, **kwargs)
            return val
        setattr(stbf, "decay", new_decay)
        stbf._setup_decay()
        stbf.start()

        print "State of blooms: %d blooms with expected error %.2f%%" % (len(stbf.blooms), stbf.expected_error()*100.)

        with TimingBlock("Adding %d values" % N, N):
            for i in xrange(N):
                stbf.add(str(i))
        last_insert = time.time()

        print "State of blooms: %d blooms with expected error %.2f%%" % (len(stbf.blooms), stbf.expected_error()*100.)

        with TimingBlock("Testing %d positive values" % N, N):
            for i in xrange(N):
                assert str(i) in stbf

        with TimingBlock("Testing %d negative values" % N, N):
            err = 0
            for i in xrange(N, 2*N):
                if str(i) in stbf:
                    err += 1
            tot_err = err / float(N)
            assert tot_err <= stbf.error, "Error is too high: %f > %f" % (tot_err, stbf.error)

        try:
            t = T - (time.time() - last_insert) + 1
            if t > 0:
                self.wait(timeout = t)
        except:
            pass

        print "State of blooms: %d blooms with expected error %.2f%%" % (len(stbf.blooms), stbf.expected_error()*100.)

        with TimingBlock("Testing %d expired values" % N, N):
            err = 0
            for i in xrange(N):
                if str(i) in stbf:
                    err += 1
            tot_err = err / float(N)
            assert tot_err <= stbf.error, "Error is too high: %f > %f" % (tot_err, stbf.error)

        assert len(stbf.blooms) == 0, "Decay should have pruned all but one bloom filters: %d blooms left" % len(stbf.blooms)

        stbf.add("test")
        assert "test" in stbf
