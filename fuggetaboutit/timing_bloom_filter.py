#!/usr/bin/env python

from counting_bloom_filter import CountingBloomFilter
import tornado.ioloop
import tornado.testing
import struct
import time

try:
    import _optimizations
except ImportError:
    print "Could not load optimizations"
    _optimizations = None


class TimingBloomFilter(CountingBloomFilter):
    @property
    def _ENTRIES_PER_8BYTE(self):
        if _optimizations:
            return 2
        return 1

    def __init__(self, *args, **kwargs):
        self.decay_time = kwargs.pop("decay_time", None)
        ioloop = kwargs.pop("ioloop", None)
        assert self.decay_time is not None, "Must provide decay_time parameter"

        super(TimingBloomFilter, self).__init__(*args, **kwargs)
        self.ring_size = None
        self.dN = None
        self.seconds_per_tick = None
        self.num_non_zero = 0

        self._initialize()
        self.set_ioloop(ioloop)

    def _initialize(self):
        self.ring_size = (1 << (8 / self._ENTRIES_PER_8BYTE)) - 1
        self.dN = self.ring_size / 2
        self.seconds_per_tick = self.decay_time / float(self.dN)
        self._optimize = _optimizations is not None

    def set_ioloop(self, ioloop=None):
        self._ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self._setup_decay()

    def _setup_decay(self):
        self.time_per_decay = self.seconds_per_tick * 1000.0 / 2.0
        try:
            self.stop()
        except:
            pass
        self._callbacktimer = tornado.ioloop.PeriodicCallback(self.decay, self.time_per_decay, self._ioloop)

    def _tick(self, timestamp=None):
        return int(((timestamp or time.time()) // self.seconds_per_tick) % self.ring_size) + 1

    def _tick_range(self):
        tick_max = self._tick()
        tick_min = (tick_max - self.dN - 1) % self.ring_size + 1
        return tick_min, tick_max

    def _test_interval(self):
        tick_min, tick_max = self._tick_range()
        if tick_min < tick_max:
            return lambda x : x and tick_min < x <= tick_max
        else:
            return lambda x : x != 0 and not tick_max < x <= tick_min

    def add(self, key, timestamp=None):
        tick = self._tick(timestamp)
        if timestamp:
            if timestamp < time.time() - self.decay_time:
                return self
        if self._optimize and self.data.flags['C_CONTIGUOUS']:
            self.num_non_zero += _optimizations.timing_bloom_add(self.data, self._indexes(key), tick)
        else:
            for index in self._indexes(key):
                self.num_non_zero += (self.data[index] == 0)
                self.data[index] = tick
        return self

    def contains(self, key):
        """
        Check if the current bloom contains the key `key`
        """
        if self._optimize and self.data.flags['C_CONTIGUOUS']:
            tick_min, tick_max = self._tick_range()
            return bool(_optimizations.timing_bloom_contains(self.data, self._indexes(key), tick_min, tick_max))
        else:
            test_interval = self._test_interval()
            return all(test_interval(self.data[index]) for index in self._indexes(key))

    def decay(self):
        if self._optimize and self.data.flags['C_CONTIGUOUS']:
            tick_min, tick_max = self._tick_range()
            self.num_non_zero = _optimizations.timing_bloom_decay(self.data, tick_min, tick_max)
        else:
            test_interval = self._test_interval()
            self.num_non_zero = 0
            for i in xrange(self.num_bytes):
                value = self.data[i]
                if value != 0:
                    if not test_interval(value):
                        self.data[i] = 0
                    else:
                        self.num_non_zero += 1

    def start(self):
        assert not self._callbacktimer._running, "Decay timer already running"
        self._callbacktimer.start()
        return self

    def stop(self):
        assert self._callbacktimer._running, "Decay timer not running"
        self._callbacktimer.stop()
        return self

    TIMING_HEADER= "dQ"
    def tofile(self, f):
        header = struct.pack(self.TIMING_HEADER, self.decay_time, self.num_non_zero)
        f.write(header)
        super(TimingBloomFilter, self).tofile(f)

    @classmethod
    def fromfile(cls, f):
        """
        Reads the bloom from the given fileobject and returns the python object
        """
        sizeof_header = struct.calcsize(cls.TIMING_HEADER)
        header = f.read(sizeof_header)
        decay_time, num_non_zero = struct.unpack(cls.TIMING_HEADER, header)
        self = super(TimingBloomFilter, cls).fromfile(f)

        self.decay_time = decay_time
        self.num_non_zero = num_non_zero
        self._ioloop = None
        self._initialize()
        self._setup_decay()
        return self

    def remove(self, *args, **kwargs):
        raise NotImplementedError

    def remove_all(self, *args, **kwargs):
        raise NotImplementedError

if __name__ == "__main__":
    from utils import TimingBlock
    N = int(1e5)
    T = 10

    print "Initializing"
    tbf = TimingBloomFilter(2*N, decay_time=T)

    with TimingBlock("Adding", N):
        for i in xrange(N):
            tbf.add("idx_%d" % i)
    start = time.time()

    with TimingBlock("Contains(+)", N):
        for i in xrange(N):
            assert tbf.contains("idx_%d" % i), "False negative"

    with TimingBlock("Contains(-)", N):
        c = 0
        for i in xrange(N, 2*N):
            c += tbf.contains("idx_%d" % i)
        assert c / float(N) <= tbf.error, "Too many false positives"

    print "Waiting for decay"
    tbf.decay()
    time.sleep(T - time.time() + start + 1)
    with TimingBlock("Decaying in C", 1):
        tbf.decay()

    print "Number of non-zero entries: ", tbf.num_non_zero

    with TimingBlock("Contains(expired)", N):
        c = 0
        for i in xrange(N):
            c += tbf.contains("idx_%d" % i)
        assert c / float(N) <= tbf.error, "Too many false positives"
