#!/usr/bin/env python

import tornado.ioloop
import tornado.testing
import tornado.gen
import numpy as np
import time
import math
import h5py
import mmh3


class TimingBloomFilterDisk(object):
    def __init__(self, capacity, decay_time, filename, error=0.005, dtype='uint8', ioloop=None):
        self.capacity = capacity
        self.error = error
        self.dtype = dtype
        self.bin_size = np.dtype(dtype).itemsize
        self.num_hashes = int(-math.log(error) / math.log(2)**3) + 1
        self.num_bytes_per_hash = int(-capacity * math.log(error) / math.log(2)**2 / self.num_hashes) + 1
        self.num_bytes = self.num_bytes_per_hash * self.num_hashes

        self.decay_time = decay_time
        self._ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.ring_size = None
        self.dN = None
        self.dt = None
        self.initialize()

        self.filename = filename
        self.data = None
        self._setup_h5py()
        self._setup_decay()

    def _setup_h5py(self):
        self.datafile = h5py.File(self.filename, 'a')
        if '/data/bloom' in self.datafile:
            self.data = self.datafile['/data/bloom']
        else:
            data = self.datafile.require_group("data")
            self.data = data.create_dataset("bloom", shape=(1e10,), dtype=self.dtype, fillvalue=0, chunks=True)
        

    def initialize(self):
        self.ring_size = (1 << (self.bin_size * 8)) - 1
        self.dN = self.ring_size / 2
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
            yield (h1 + i * h2) % self.num_bytes_per_hash + i*self.num_bytes_per_hash

    def add(self, key):
        assert isinstance(key, str)
        tick = self._tick()
        indexes = tuple(self._indexes(key))
        self.data[indexes,] = tick
        return self

    def contains(self, key):
        """
        Check if the current bloom contains the key `key`
        """
        assert isinstance(key, str)
        test_interval = self._test_interval()
        indexes = tuple(self._indexes(key))
        return all(test_interval(d) for d in self.data[indexes,])


    @tornado.gen.coroutine
    def decay(self):
        for i in range(self.num_bytes // 1000 + 1):
            test_interval = self._test_interval()
            imin = i * 1000
            imax = (i+1) * 1000

            data = self.data[imin:imax]
            for j in xrange(data.shape[0]):
                if not test_interval(self.data[i]):
                    self.data[i] = 0
            yield self._ioloop.add_callback()

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
    N = int(1e5)
    n = int(5e3)

    tbf = TimingBloomFilterDisk(2*N, 15, "test_bloom.hdf5")

    print "Adding"
    with test.TimingBlock("Adding", n):
        for i in xrange(n):
            tbf.add("idx_%d" % i)

    print "Checking"
    with test.TimingBlock("Contains", n):
        imin = int(0.5 * n)
        imax = int(1.5 * n)
        for i in xrange(imin, imax):
            tbf.contains("idx_%d" % i)

