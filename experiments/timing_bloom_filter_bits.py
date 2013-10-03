#!/usr/bin/env python

import tornado.ioloop
import tornado.testing
import tornado.gen
import bitarray
import time
import math
import mmh3

class BitArray(object):
    def __init__(self, nitems, nbits=4):
        self.data = bitarray.bitarray(nitems * nbits)
        self.data.setall(False)
        self.nitems = nitems
        self.nbits = nbits
        self.maxval = 1 << nbits

    def __getitem__(self, i):
        n = 0
        imin = i * self.nbits
        imax = (i + 1) * self.nbits
        for v in reversed(self.data[imin:imax]):
            n += v
            n <<= 1
        n >>= 1
        return n

    def __setitem__(self, i, v):
        if not 0 <= v < self.maxval:
            ValueError()

        imin = i * self.nbits
        imax = (i + 1) * self.nbits

        if v == 0:
            self.data[imin:imax] = 0
        elif v == self.maxval:
            self.data[imin:imax] = 1
        else:
            for j in range(imin, imax):
                self.data[j] = v & 1
                v >>= 1


class TimingBloomFilterBits(object):
    def __init__(self, capacity, decay_time, error=0.005, bits=4, ioloop=None):
        self.capacity = capacity
        self.error = error
        self.bits = bits
        self.num_bins = int(-capacity * math.log(error) / math.log(2)**2) + 1
        self.num_hashes = int(self.num_bins / capacity * math.log(2)) + 1

        self.data = BitArray(self.num_bins, nbits=bits)

        self.decay_time = decay_time
        self._ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.ring_size = None
        self.dN = None
        self.dt = None
        self.initialize()

    def initialize(self):
        self.ring_size = (1 << self.bits) - 1
        self.dN = self.ring_size // 2
        self.dt = self.decay_time / float(self.dN)

    def _setup_decay(self):
        t = self.dt * 1000.0 / 2.0
        self._callbacktimer = tornado.ioloop.PeriodicCallback(self.decay, t, self._ioloop)

    def _tick(self):
        return int((time.time() // self.dt) % self.ring_size) + 1

    def _tick_range(self):
        tick_max = self._tick()
        tick_min = (tick_max - self.dN - 1) % self.ring_size + 1
        return tick_min, tick_max

    def _test_interval(self):
        tick_min, tick_max = self._tick_range()
        if tick_min < tick_max:
            return lambda x : x and tick_min < x <= tick_max
        else:
            return lambda x : 0 < x <= tick_max or tick_min < x < self.ring_size

    def _indexes(self, key):
        for i in xrange(self.num_hashes):
            h1, h2 = mmh3.hash64(key)
            yield (h1 + i * h2) % self.num_bins

    def add(self, key):
        assert isinstance(key, str)
        tick = self._tick()
        for i in self._indexes(key):
            self.data[i] = tick
        return self

    def contains(self, key):
        """
        Check if the current bloom contains the key `key`
        """
        assert isinstance(key, str)
        test_interval = self._test_interval()
        return all(test_interval(self.data[i]) for i in self._indexes(key))


    def decay(self):
        test_interval = self._test_interval()
        for i in self.num_bins:
            if not test_interval(self.data[i]):
                self.data[i] = 0

    def start(self):
        assert not self._callbacktimer._running, "Decay timer already running"
        self._callbacktimer.start()
        return self

    def stop(self):
        assert self._callbacktimer._running, "Decay timer not running"
        self._callbacktimer.stop()
        return self


if __name__ == "__main__":
    import test
    N = int(1e8)
    n = int(1e5)

    with test.TimingBlock("Initializing"):
        tbf = TimingBloomFilterBits(2*N, decay_time=30)

    with test.TimingBlock("Adding", n):
        for i in xrange(n):
            tbf.add("idx_%d" % i)

    with test.TimingBlock("Contains(+)", n):
        for i in xrange(n):
            assert tbf.contains("idx_%d" % i), "False negative: idx_%d" % i

    with test.TimingBlock("Contains(-)", n):
        c = 0
        for i in xrange(N, N+n):
            c += tbf.contains("idx_%d" % i)
        assert c / float(N) <= tbf.error, "Too many false positives"

