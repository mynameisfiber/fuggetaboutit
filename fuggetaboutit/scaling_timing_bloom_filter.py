#!/usr/bin/env python

import tornado.ioloop
from timing_bloom_filter import TimingBloomFilter
import math
import struct
import operator

class ScalingTimingBloomFilter(object):
    """
    A bloom filter that will decay old values and scale up capacity as
    needed.  This bloom filter will automatically decay values such that an
    item added will be removed from the bloom after ``decay_time`` seconds.
    The underlying bloom has initial capacity ``capacity`` and maximum error
    ``error``.  In addition, the bloom will automatically scale using
    Almeida's method from "Scalable Bloom Filters" using an error tightning
    ratio and growth factor given by ``error_tightning_ratio`` and
    ``growth_factor``.  A bloom filter will be scaled up when
    ``max_load_factor`` percent of the underlying bits of the bloom filter
    are in use.  This value can be a maximum on ln(2) since this would
    represent ``capacity`` number of elements being inserted.
    
    :param capacity: initial capacity
    :type capacity: integer
    
    :param decay_time: time, in seconds, for an item to decay
    :type decay_time: integer
    
    :param error: maximum error rate
    :type error: float
    
    :param error_tightning_ratio: reduction factor for the error rate of scaled blooms
    :type error_tightning_ratio: float
    
    :param growth_factor: increasing factor for the capacity of scaled blooms
    :type growth_factor: float or None
    
    :param max_load_factor: maximum load factor of a bloom to be considered full
    :type max_load_factor: 0 < float < ln(2)
    
    :param num_buffer_blooms: number of spare blooms to keep to allow for frequent scaling
    :type num_buffer_blooms: integer
    
    :param ioloop: an instance of an IOLoop to attatch the periodic decay operation to
    :type ioloop: tornado.ioloop.IOLoop or None
    """
    def __init__(self, capacity, decay_time, error=0.005, 
            error_tightning_ratio = 0.5, growth_factor=None, 
            max_load_factor=0.6, num_buffer_blooms = 1, 
            ioloop=None):
        assert 0 < max_load_factor <= math.log(2), "max_load_factor must be 0<max_load_factor<=ln(2)"
        assert growth_factor is None or 0 < growth_factor, "growth_factor must be None or >0"
        assert 0 <= num_buffer_blooms, "num_buffer_blooms must be >=0"
        assert 0 < error < 1, "error must be 0 < error < 1"

        self.max_load_factor = max_load_factor

        self.error = error
        self.error_tightning_ratio = error_tightning_ratio
        self.error_initial = self.error * (1 - self.error_tightning_ratio)
        self.growth_factor = growth_factor

        self.capacity = capacity
        self.decay_time = decay_time
        self.max_id = 0
        self.num_buffer_blooms = num_buffer_blooms

        self.blooms = [ ]
        self._add_new_bloom()
        self.max_load_factor_raw = int(self.blooms[0]["bloom"].num_hashes * capacity * max_load_factor)

        self.set_ioloop(ioloop)


    def set_ioloop(self, ioloop=None):
        """
        Set a new IOLoop to attatch the decay operation to.  ``stop()`` should be
        called before changing the ioloop.

        :param ioloop: an instance of an IOLoop to attatch the periodic decay operation to
        :type ioloop: tornado.ioloop.IOLoop or None
        """
        self._ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self._setup_decay()

    def _setup_decay(self):
        self.ring_size = (1 << 8) - 1
        self.dN = self.ring_size / 2
        self.seconds_per_tick = self.decay_time / float(self.dN)
        self.time_per_decay = self.seconds_per_tick * 1000.0 / 2.0
        try:
            self.stop()
        except:
            pass
        self._callbacktimer = tornado.ioloop.PeriodicCallback(self.decay, self.time_per_decay, self._ioloop)

    def _add_new_bloom(self):
        error = self.error_initial * self.error_tightning_ratio ** len(self.blooms)
        if self.growth_factor:
            capacity = math.log(2) * self.capacity * self.growth_factor ** len(self.blooms)
        else:
            capacity = self.capacity
        self.blooms.append({
            "id" : self.max_id,
            "error" : error, 
            "capacity" : capacity,
            "bloom" : TimingBloomFilter(
                capacity = capacity, 
                decay_time = self.decay_time, 
                error = error, 
            ),
        })
        self.max_id += 1

    def expected_error(self):
        """
        Return the current expected error rate from the blooms.  This should
        always be smaller than the maximum error given in the constructor.

        :rtype: float
        """
        return 1 - reduce(operator.mul, (1 - bloom["error"] for bloom in self.blooms))

    def add(self, key, timestamp=None):
        """
        Add key to the bloom filter and scale if necissary.  The key will be
        inserted at the current timestamp if parameter ``timestamp`` is not
        provided.

        :param key: key to be added
        :type key: str

        :param timestamp: timestamp of the item
        :type timestamp: int
        """
        cur_bloom = None
        for bloom in self.blooms:
            if bloom["bloom"].num_non_zero < self.max_load_factor_raw:
                cur_bloom = bloom
                break
        if cur_bloom is None:
            self._add_new_bloom()
            cur_bloom = self.blooms[-1]
        cur_bloom["bloom"].add(key, timestamp)
        return self

    def contains(self, key):
        """
        Check if key is contained in the bloom filter

        :param key: key to be added
        :type key: str

        :rtype: bool
        """
        for bloom in self.blooms:
            if key in bloom["bloom"]:
                return True
        return False

    def decay(self):
        """
        Decay the bloom filter and remove items that are older than
        ``decay_time``.  This will also remove empty bloom filters.
        """
        num_empty_blooms = 0
        for bloom in self.blooms:
            bloom["bloom"].decay()
            num_empty_blooms += (bloom["bloom"].num_non_zero == 0)
        if num_empty_blooms > self.num_buffer_blooms:
            to_remove = min(num_empty_blooms - self.num_buffer_blooms, len(self.blooms) - 1)
            for i in reversed(xrange(len(self.blooms))):
                bloom = self.blooms[i]
                if bloom["bloom"].num_non_zero == 0:
                    item = self.blooms.pop(i)
                    del item["bloom"]
                    to_remove -= 1
                if not to_remove:
                    break

    def start(self):
        """
        Start a periodic callback on the IOLoop to decay the bloom at every
        half ``decay_time``.
        """
        assert not self._callbacktimer._running, "Decay timer already running"
        self._callbacktimer.start()
        return self

    def stop(self):
        """
        Stop the periodic decay operation
        """
        assert self._callbacktimer._running, "Decay timer not running"
        self._callbacktimer.stop()
        return self

    def tofile(self, f):
        """
        Saves the current bloom to the file object ``f``.

        :param f: File to save the bloom filter to
        :type f: file
        """
        header = struct.pack("QdddddQQdQ", self.capacity, self.decay_time, self.error, self.error_tightning_ratio, self.growth_factor or -1, self.max_load_factor, self.num_buffer_blooms, len(self.blooms), self.error_initial, self.max_load_factor_raw)
        f.write(header + "\n")
        for bloom in self.blooms:
            bheader = struct.pack("Qdd", bloom["id"], bloom["error"], bloom["capacity"])
            f.write(bheader + "\n")
            bloom["bloom"].tofile(f)

    @classmethod
    def fromfile(cls, f):
        """
        Reads the bloom from the given file ``f``.

        :param f: File to save the bloom filter to
        :type f: file

        :rtype: ScalingTimingBloomFilter
        """
        self = cls.__new__(cls)
        header = f.readline()[:-1]
        self.capacity, self.decay_time, self.error, self.error_tightning_ratio, self.growth_factor, self.max_load_factor, self.num_buffer_blooms, N, self.error_initial, self.max_load_factor_raw = struct.unpack("QdddddQQdQ", header)

        if self.growth_factor == -1:
            self.growth_factor = None

        self.blooms = [ ]
        self.max_id = 0
        for i in range(N):
            bheader = f.readline()[:-1]
            _id, error, capacity = struct.unpack("Qdd", bheader)
            bloom = TimingBloomFilter.fromfile(f)
            self.blooms.append({
                "id" : _id,
                "error" : error,
                "capacity" : capacity,
                "bloom" : bloom,
            })
            self.max_id = max(self.max_id, _id)

        self._ioloop = None
        self._setup_decay()    
        return self

    def __contains__(self, key):
        return self.contains(key)

    def __add__(self, other):
        return self.add(other)


if __name__ == "__main__":
    from utils import TimingBlock
    import time
    N = int(500)
    decay_time = 5

    print "Creating ScalingTimingBloomFilter with capacity=%d and decay_time=%f" % (N, decay_time)
    with TimingBlock("Initialization"):
        tbf = ScalingTimingBloomFilter(N, decay_time=decay_time)

    with TimingBlock("Adding", 2*N):
        for i in xrange(2*N):
            tbf.add("idx_%d" % i)
    last_insert = time.time()

    print "State of blooms: %d blooms with expected error %.2f%%" % (len(tbf.blooms), tbf.expected_error()*100.)

    with TimingBlock("Contains(+)", 2*N):
        for i in xrange(2*N):
            assert tbf.contains("idx_%d" % i), "False negative"

    with TimingBlock("Contains(-)", N):
        c = 0
        for i in xrange(2*N, 3*N):
            c += tbf.contains("idx_%d" % i)
        assert c / float(N) <= tbf.error, "Too many false positives: %f > %f" % (c/float(N), tbf.error)

    with TimingBlock("Decaying", 1):
        tbf.decay()
    time.sleep(decay_time - time.time() + last_insert) 
    with TimingBlock("Decaying", 1):
        tbf.decay()

    print "State of blooms: %d blooms with expected error %.2f%%" % (len(tbf.blooms), tbf.expected_error()*100.)

    with TimingBlock("Contains(-)", 2*N):
        c = 0
        for i in xrange(N, 2*N):
            c += tbf.contains("idx_%d" % i)
        assert c / float(N) <= tbf.error, "Too many false positives: %f > %f" % (c/float(N), tbf.error)
