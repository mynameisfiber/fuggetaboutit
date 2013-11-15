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
    Almeida's method from "Scalable Bloom Filters" using an error tightening
    ratio and growth factor given by ``error_tightening_ratio`` and
    ``growth_factor``.  A bloom filter will be scaled up when approximatly
    ``max_fill_factor`` percent of the capacity of the bloom filter are in use.
    Conversely, the bloom will be scaled down if there is one bloom left and it
    has a fill percentage less than ``min_fill_factor``.  Together, these two
    fill factor conditionals attempt to keep the scaling bloom at the right
    size for the current stream of data.
    
    :param capacity: initial capacity
    :type capacity: integer
    
    :param decay_time: time, in seconds, for an item to decay
    :type decay_time: integer
    
    :param error: maximum error rate
    :type error: float
    
    :param error_tightening_ratio: reduction factor for the error rate of scaled blooms
    :type error_tightening_ratio: float
    
    :param growth_factor: increasing factor for the capacity of scaled blooms
    :type growth_factor: float or None
    
    :param max_fill_factor: maximum fill factor of a bloom to be considered full
    :type max_fill_factor: min_fill_factor < float < 1

    :param max_fill_factor: minimum fill factor of a bloom to be considered active
    :type max_fill_factor: 0 < float < max_fill_factor or None
    
    :param insert_order: Whether to insert in order to optimize compactness or convergence
    :type insert_order: 'compact' or 'converge'

    :param ioloop: an instance of an IOLoop to attatch the periodic decay operation to
    :type ioloop: tornado.ioloop.IOLoop or None
    """
    def __init__(self, capacity, decay_time, error=0.005,
            error_tightening_ratio=0.5, growth_factor=2, min_fill_factor=0.2,
            max_fill_factor=0.8, insert_order='converge', ioloop=None):
        assert (0 or min_fill_factor) < max_fill_factor <= 1, "max_fill_factor must be min_fill_factor<max_fill_factor<=1"
        assert min_fill_factor is None or 0 < min_fill_factor < max_fill_factor, "min_fill_factor must be None or 0<min_fill_factor<max_fill_factor"
        assert growth_factor is None or 0 < growth_factor, "growth_factor must be None or >0"
        assert 0 < error < 1, "error must be 0 < error < 1"

        self.error = error
        self.error_tightening_ratio = error_tightening_ratio
        self.error_initial = self.error * (1 - self.error_tightening_ratio)
        self.growth_factor = growth_factor

        self.capacity = capacity
        self.decay_time = decay_time

        self.blooms = [ ]
        self._add_new_bloom()

        self.max_fill_factor = max_fill_factor
        self.min_fill_factor = min_fill_factor

        self.insert_tail = (insert_order == 'converge')

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

    def _get_next_id(self):
        if self.blooms:
            max_id = max(b["id"] for b in self.blooms)
            return max_id + 1
        return 0

    def _add_new_bloom(self, _id=None):
        _id = _id or self._get_next_id()
        error = self.error_initial * self.error_tightening_ratio ** _id
        if self.growth_factor:
            capacity = int(math.log(2) * self.capacity * self.growth_factor ** _id)
        else:
            capacity = self.capacity
        self.blooms.append({
            "id" : _id,
            "new" : True,
            "error" : error, 
            "capacity" : capacity,
            "bloom" : TimingBloomFilter(
                capacity = capacity, 
                decay_time = self.decay_time, 
                error = error, 
            ),
        })

    def size(self):
        """
        Returns the approximate size of the current bloom state.

        :rtype: float
        """
        return sum(b["bloom"].size() for b in self.blooms)

    def expected_error(self):
        """
        Return the current expected error rate from the blooms.  This should
        always be smaller than the maximum error given in the constructor.

        :rtype: float
        """
        if not self.blooms:
            return 0.0
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
        bloom_iter = None
        if self.insert_tail:
            bloom_iter = reversed(self.blooms)
        else:
            bloom_iter = iter(self.blooms)
        for bloom in bloom_iter:
            if bloom["bloom"].size() < self.max_fill_factor * bloom["capacity"]:
                cur_bloom = bloom
                break
        if cur_bloom is None:
            self._add_new_bloom()
            cur_bloom = self.blooms[-1]
        cur_bloom["bloom"].add(key, timestamp)
        cur_bloom["new"] = False
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

        if num_empty_blooms:
            to_remove = num_empty_blooms
            possible_blooms = sorted(enumerate(self.blooms), key=lambda b : b[1]["capacity"] / b[1]["error"], reverse=True)
            for i, bloom in possible_blooms:
                if bloom["bloom"].num_non_zero == 0 and not bloom["new"]:
                    item = self.blooms.pop(i)
                    del item["bloom"]
                    to_remove -= 1
                if not to_remove:
                    break

        # In the case that there is only one active bloom filter, check if it's
        # load factor is <= the minimum load factor and, if so, create a new
        # bloom with a smaller capacity
        if self.min_fill_factor and len(self.blooms) == 1:
            bloom = self.blooms[0]
            if 0 < bloom["bloom"].size() < self.min_fill_factor * bloom["capacity"]:
                max_id = bloom["id"]
                if max_id > 0:
                    self._add_new_bloom(max_id - 1)

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

    SCALING_TIMING_HEADER = "QddddddQd"
    SCALING_TIMING_HEADER_BLOOM = "Qdd"
    def tofile(self, f):
        """
        Saves the current bloom to the file object ``f``.

        :param f: File to save the bloom filter to
        :type f: file
        """
        header = struct.pack(self.SCALING_TIMING_HEADER, self.capacity, self.decay_time, self.error, self.error_tightening_ratio, self.growth_factor or -1, self.max_fill_factor, self.min_fill_factor or -1, len(self.blooms), self.error_initial)
        f.write(header)
        for bloom in self.blooms:
            bheader = struct.pack(self.SCALING_TIMING_HEADER_BLOOM, bloom["id"], bloom["error"], bloom["capacity"])
            f.write(bheader)
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
        sizeof_header = struct.calcsize(cls.SCALING_TIMING_HEADER)
        sizeof_header_bloom = struct.calcsize(cls.SCALING_TIMING_HEADER_BLOOM)

        header = f.read(sizeof_header)
        self.capacity, self.decay_time, self.error, self.error_tightening_ratio, self.growth_factor, self.max_fill_factor, self.min_fill_factor, N, self.error_initial = struct.unpack(cls.SCALING_TIMING_HEADER, header)

        if self.min_fill_factor == -1:
            self.growth_factor = None
        if self.growth_factor == -1:
            self.growth_factor = None

        self.blooms = [ ]
        for i in range(N):
            bheader = f.read(sizeof_header_bloom)
            _id, error, capacity = struct.unpack(cls.SCALING_TIMING_HEADER_BLOOM, bheader)
            bloom = TimingBloomFilter.fromfile(f)
            self.blooms.append({
                "id" : _id,
                "error" : error,
                "capacity" : capacity,
                "bloom" : bloom,
                "new" : False,
            })

        self.insert_tail = True #TODO: have this be in the file
        self._ioloop = None
        self._setup_decay()    
        return self

    def __contains__(self, key):
        return self.contains(key)

    def __add__(self, other):
        return self.add(other)

    def __len__(self):
        return self.size()


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
