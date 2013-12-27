#!/usr/bin/env python

import json
import logging
import math
import operator
import os
from shutil import rmtree

from .tickers import NoOpTicker
from .timing_bloom_filter import TimingBloomFilter, _ENTRIES_PER_8BYTE

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
    
    :param insert_tail: Whether to insert in order to optimize compactness or convergence
    :type insert_tail: True (convergence) or False (compactness)

    :param ioloop: an instance of an IOLoop to attatch the periodic decay operation to
    :type ioloop: tornado.ioloop.IOLoop or None
    """
    def __init__(self, capacity, decay_time, ticker=None, data_path=None, error=0.005,
            error_tightening_ratio=0.5, growth_factor=2, min_fill_factor=0.2,
            max_fill_factor=0.8, insert_tail=True, blooms=None):
        assert (0 or min_fill_factor) < max_fill_factor <= 1, "max_fill_factor must be min_fill_factor<max_fill_factor<=1"
        assert min_fill_factor is None or 0 < min_fill_factor < max_fill_factor, "min_fill_factor must be None or 0<min_fill_factor<max_fill_factor"
        assert growth_factor is None or 0 < growth_factor, "growth_factor must be None or >0"
        assert 0 < error < 1, "error must be 0 < error < 1"

        self.error = error
        self.error_tightening_ratio = error_tightening_ratio
        self.error_initial = self.error * (1 - self.error_tightening_ratio)
        self.growth_factor = growth_factor
        self.data_path = data_path
        self.meta_filename = os.path.join(data_path, 'meta.json')
        self.blooms_path = os.path.join(data_path, 'blooms')

        self.capacity = capacity
        self.decay_time = decay_time

        if blooms:
            self.blooms = blooms
        else:
            self.blooms = [ ]
            self._add_new_bloom()

        self.max_fill_factor = max_fill_factor
        self.min_fill_factor = min_fill_factor

        self.insert_tail = insert_tail

        if ticker is None:
            self.ticker = NoOpTicker()
        else:
            self.ticker = ticker

        self._setup_decay()


    def _setup_decay(self):
        self.ring_size = (1 << 8 / _ENTRIES_PER_8BYTE) - 1
        self.dN = self.ring_size / 2
        self.seconds_per_tick = self.decay_time / float(self.dN)
        self.time_per_decay = self.seconds_per_tick * 1000.0 
        self.ticker.setup(self.decay, self.time_per_decay)
        self.ticker.start()

    def _get_next_id(self):
        if self.blooms:
            max_id = max(b.id for b in self.blooms)
            return max_id + 1
        return 0

    def _add_new_bloom(self, bloom_id=None):
        bloom_id = bloom_id or self._get_next_id()
        error = self.error_initial * self.error_tightening_ratio ** bloom_id
        if self.growth_factor:
            capacity = int(math.log(2) * self.capacity * self.growth_factor ** bloom_id)
        else:
            capacity = self.capacity
        self.blooms.append(TimingBloomFilter(
            capacity=capacity,
            decay_time=self.decay_time,
            error=error,
            id=bloom_id,
            data_path=os.path.join(self.blooms_path, str(bloom_id))
        ))

    def size(self):
        """
        Returns the approximate size of the current bloom state.

        :rtype: float
        """
        return sum(b.size() for b in self.blooms)

    def expected_error(self):
        """
        Return the current expected error rate from the blooms.  This should
        always be smaller than the maximum error given in the constructor.

        :rtype: float
        """
        if not self.blooms:
            return 0.0
        return 1 - reduce(operator.mul, (1 - bloom.error for bloom in self.blooms))

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

        bloom_count = len(self.blooms)
        logging.debug("Available Blooms:  %d" % bloom_count)
        for n, bloom in enumerate(bloom_iter):
            if self.insert_tail:
                n = bloom_count - n
            logging.debug("Checking if bloom %d has available capacity..." % n)
            size = bloom.size()
            logging.debug("size:  %r" % size)
            logging.debug("max_fill_factor:  %r" % self.max_fill_factor)
            logging.debug("capacity:  %r" % bloom.capacity)

            if size < self.max_fill_factor * bloom.capacity:
                logging.debug("Bloom %d has available capacity." % n)
                cur_bloom = bloom
                break

        if cur_bloom is None:
            logging.debug("No available blooms, adding new bloom")
            self._add_new_bloom()
            cur_bloom = self.blooms[-1]
        cur_bloom.add(key, timestamp)
        return self

    def contains(self, key):
        """
        Check if key is contained in the bloom filter

        :param key: key to be added
        :type key: str

        :rtype: bool
        """
        for bloom in self.blooms:
            if key in bloom:
                return True
        return False

    def decay(self):
        """
        Decay the bloom filter and remove items that are older than
        ``decay_time``.  This will also remove empty bloom filters.
        """
        num_empty_blooms = 0
        for bloom in self.blooms:
            bloom.decay()
            num_empty_blooms += bloom["bloom"].num_non_zero == 0

        if num_empty_blooms:
            to_remove = num_empty_blooms
            possible_blooms = sorted(reversed(self.blooms), key=lambda b : b.capacity / b.error)
            for i, bloom in possible_blooms:
                if bloom.num_non_zero == 0:
                    rmtree(bloom.data_path)
                    del self.blooms[i]
                    to_remove -= 1

                    if to_remove <= 0:
                        break

        # In the case that there is only one active bloom filter, check if it's
        # load factor is <= the minimum load factor and, if so, create a new
        # bloom with a smaller capacity
        if self.min_fill_factor and len(self.blooms) == 1:
            bloom = self.blooms[0]
            if 0 < bloom.size() < self.min_fill_factor * bloom.capacity:
                max_id = bloom.id
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

    def get_meta(self):
        return {
            'capacity': self.capacity,
            'decay_time': self.decay_time,
            'error': self.error,
            'error_tightening_ratio': self.error_tightening_ratio,
            'growth_factor': self.growth_factor,
            'min_fill_factor': self.min_fill_factor,
            'max_fill_factor': self.max_fill_factor,
            'insert_tail': self.insert_tail,
        }

    def save(self):
        if not os.path.exists(self.data_path):
            logging.info("Data path doesn't exist, creating:  %s" % self.data_path)
            os.makedirs(self.data_path)

        meta = self.get_meta()
        with open(self.meta_filename, 'w') as meta_file:
            json.dump(meta, meta_file)

        for bloom in self.blooms:
            bloom.save()


    @classmethod
    def load(cls, data_path, ticker):
        logging.debug("Loading bloom from %s" % data_path)
        meta_filename = os.path.join(data_path, 'meta.json')
        blooms_path = os.path.join(data_path, 'blooms')
        
        kwargs = {'data_path': data_path, 'ticker': ticker}

        with open(meta_filename, 'r') as meta_file:
            kwargs.update(json.load(meta_file))

        blooms = []
        for bloom_path in os.listdir(blooms_path):
            if not os.path.isdir(bloom_path):
                continue

            logging.debug("Loading a sub bloom from %s" % bloom_path)
            blooms.append(TimingBloomFilter.load(bloom_path))

        kwargs['blooms'] = blooms
        return cls(**kwargs)

    def __contains__(self, key):
        return self.contains(key)

    def __add__(self, other):
        return self.add(other)

    def __len__(self):
        return self.size()
