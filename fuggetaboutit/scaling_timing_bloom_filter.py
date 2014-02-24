#!/usr/bin/env python

import json
import logging
import math
import operator
import os
from shutil import rmtree

from .exceptions import PersistenceDisabledException
from .tickers import NoOpTicker
from .timing_bloom_filter import TimingBloomFilter

META_FILENAME = 'meta.json'
BLOOMS_PATH = 'blooms'

def _get_paths(obj_data_path, arg_data_path):
    if not (obj_data_path or arg_data_path):
        raise PersistenceDisabledException("You cannot save without having data_path set.")
    data_path = arg_data_path or obj_data_path

    data_path = os.path.normpath(data_path)
    meta_path = os.path.join(data_path, META_FILENAME)
    blooms_path = os.path.join(data_path, BLOOMS_PATH)
    return data_path, meta_path, blooms_path

class ScalingTimingBloomFilter(object):
    """
    A bloom filter that will decay old values and scale up capacity as
    needed.  This bloom filter will automatically decay values such that an
    item added will be removed from the bloom after ``decay_time`` seconds.
    The underlying bloom has initial ``capacity`` and maximum ``error``.
    In addition, the bloom will automatically scale using Almeida's method
    from "Scalable Bloom Filters" using ``error_tightening_ratio`` and
    ``growth_factor``.  A bloom filter will be scaled up when approximately
    ``max_fill_factor`` of the capacity of the bloom filter is in use.
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
    
    :param min_fill_factor: minimum fill factor of a bloom to be considered active
    :type min_fill_factor: 0 < float < max_fill_factor or None
    
    :param insert_tail: Whether to insert in order to optimize compactness or convergence
    :type insert_tail: True (convergence) or False (compactness)
    
    :param ioloop: an instance of an IOLoop to attach the periodic decay operation to
    :type ioloop: tornado.ioloop.IOLoop or None
    """
    def __init__(self, capacity, decay_time, ticker=None, data_path=None, error=0.005,
            error_tightening_ratio=0.5, growth_factor=2, min_fill_factor=0.2,
            max_fill_factor=0.8, insert_tail=True, blooms=None, disable_optimizations=False):
        assert (min_fill_factor or 0) < max_fill_factor <= 1, "max_fill_factor must be min_fill_factor<max_fill_factor<=1"
        assert min_fill_factor is None or 0 < min_fill_factor < max_fill_factor, "min_fill_factor must be None or 0<min_fill_factor<max_fill_factor"
        assert growth_factor is None or 0 < growth_factor, "growth_factor must be None or >0"
        assert 0 < error < 1, "error must be 0 < error < 1"

        self.error = error
        self.error_tightening_ratio = error_tightening_ratio
        self.error_initial = self.error * (1 - self.error_tightening_ratio)
        self.growth_factor = growth_factor
        self.capacity = capacity
        self.decay_time = decay_time
        self.max_fill_factor = max_fill_factor
        self.min_fill_factor = min_fill_factor
        self.insert_tail = insert_tail
        self.seconds_per_tick = None
        self.disable_optimizations = disable_optimizations

        self.data_path = None
        if data_path:
            self.data_path = os.path.normpath(data_path)

        if blooms:
            self.blooms = blooms
        else:
            self.blooms = [ ]
            self._add_new_bloom()

        if ticker is None:
            self.ticker = NoOpTicker()
        else:
            self.ticker = ticker

        self._setup_decay()


    def _setup_decay(self):
        self.seconds_per_tick = self.blooms[0].seconds_per_tick
        self.ticker.setup(self.decay, self.seconds_per_tick)
        self.ticker.start()

    def _get_next_id(self):
        if self.blooms:
            max_id = max(b.id for b in self.blooms)
            return max_id + 1
        return 0

    def get_bloom_path(self, blooms_path, bloom_id):
        return os.path.join(blooms_path, str(bloom_id))

    def get_capacity_for_id(self, bloom_id):
        if self.growth_factor:
            capacity = int(math.log(2) * self.capacity * self.growth_factor ** bloom_id)
        else:
            capacity = self.capacity

        return capacity

    def _add_new_bloom(self, bloom_id=None):
        bloom_id = bloom_id or self._get_next_id()
        error = self.error_initial * (self.error_tightening_ratio ** bloom_id)
        capacity = self.get_capacity_for_id(bloom_id)

        bloom = TimingBloomFilter(
            capacity=capacity,
            decay_time=self.decay_time,
            error=error,
            id=bloom_id,
            disable_optimizations=self.disable_optimizations,
        )
        self.blooms.append(bloom)

        return bloom

    def get_size(self):
        """
        Returns the approximate size of the current bloom state.

        :rtype: float
        """
        return sum(b.get_size() for b in self.blooms)

    def get_expected_error(self):
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
        cur_bloom = self.get_active_bloom()
        cur_bloom.add(key, timestamp)

    def get_active_bloom(self):
        cur_bloom = None
        bloom_iter = self.get_bloom_iter()

        bloom_count = len(self.blooms)
        logging.debug("Available Blooms:  %d" % bloom_count)
        for n, bloom in enumerate(bloom_iter):
            if self.insert_tail:
                n = bloom_count - n
            logging.debug("Checking if bloom %d has available capacity..." % n)
            size = bloom.get_size()
            logging.debug("size:  %r" % size)
            logging.debug("max_fill_factor:  %r" % self.max_fill_factor)
            logging.debug("capacity:  %r" % bloom.capacity)

            if size < self.max_fill_factor * bloom.capacity:
                logging.debug("Bloom %d has available capacity." % n)
                cur_bloom = bloom
                break

        if cur_bloom is None:
            logging.debug("No available blooms, adding new bloom")
            cur_bloom = self._add_new_bloom()

        return cur_bloom

    def get_bloom_iter(self):
        if self.insert_tail:
            bloom_iter = reversed(self.blooms)
        else:
            bloom_iter = iter(self.blooms)

        return bloom_iter

    def contains(self, key):
        """
        Check if key is contained in the bloom filter

        :param key: key to be added
        :type key: str

        :rtype: bool
        """
        return any(bloom.contains(key) for bloom in self.blooms)

    def decay(self):
        """
        Decay the bloom filter and remove items that are older than
        ``decay_time``.  This will also remove empty bloom filters.
        """
        for bloom in self.blooms:
            bloom.decay()
            

        self.cleanup_empty_blooms()
        self.try_to_shrink()

    def cleanup_empty_blooms(self):
        num_empty_blooms = sum(bloom.num_non_zero == 0 for bloom in self.blooms)

        if num_empty_blooms:
            to_remove = num_empty_blooms
            for i, bloom in enumerate(self.blooms):
                if bloom.num_non_zero == 0:
                    rmtree(bloom.data_path)
                    del self.blooms[i]
                    to_remove -= 1

                    if to_remove <= 0:
                        break

        return num_empty_blooms

    def try_to_shrink(self):
        """
        In the case that there is only one active bloom filter, check if its
        load factor is <= the minimum load factor and, if so, create a new
        bloom with a smaller capacity
        """
        did_shrink = False

        if self.min_fill_factor and len(self.blooms) == 1:
            bloom = self.blooms[0]
            if 0 < bloom.get_size() < self.min_fill_factor * bloom.capacity:
                max_id = bloom.id
                if max_id > 0:
                    self._add_new_bloom(max_id - 1)
                    did_shrink = True

        return did_shrink

    def start(self):
        """
        Start a periodic callback on the IOLoop to decay the bloom at every
        half ``decay_time``.
        """
        self.ticker.start()

    def stop(self):
        """
        Stop the periodic decay operation
        """
        self.ticker.stop()

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
            'disable_optimizations': self.disable_optimizations,
        }

    def save(self, data_path=None):
        data_path, meta_filename, blooms_path = _get_paths(self.data_path, data_path)

        if not os.path.exists(data_path):
            logging.debug("Data path doesn't exist, creating:  %s" % data_path)
            os.makedirs(data_path)

        meta = self.get_meta()
        with open(meta_filename, 'w') as meta_file:
            json.dump(meta, meta_file)

        for bloom in self.blooms:
            bloom.save(self.get_bloom_path(blooms_path, bloom.id))

    @classmethod
    def discover_blooms(cls, blooms_path):
        paths = []
        for bloom_path in os.listdir(blooms_path):
            full_path = os.path.join(blooms_path, bloom_path)
            if not os.path.isdir(full_path):
                continue

            logging.debug("Found a sub-bloom at '%s'" % full_path)
            paths.append(full_path)

        return paths

    @classmethod
    def load(cls, data_path, ticker=None):
        logging.debug("Loading scaling timing bloom from %s" % data_path)
        data_path, meta_filename, blooms_path = _get_paths(None, data_path)
        blooms = []
        
        kwargs = {'data_path': data_path, 'ticker': ticker}

        with open(meta_filename, 'r') as meta_file:
            kwargs.update(json.load(meta_file))

        bloom_paths = cls.discover_blooms(blooms_path)
        for path in bloom_paths:
            logging.debug("Loading sub-bloom from '%s'" % path)
            blooms.append(TimingBloomFilter.load(path))

        if not blooms:
            logging.warn("No sub-blooms found in '%s'" % blooms_path)

        kwargs['blooms'] = blooms

        capacity = kwargs['capacity']
        del kwargs['capacity']

        decay_time = kwargs['decay_time']
        del kwargs['decay_time']

        return cls(capacity, decay_time, **kwargs)

    def __contains__(self, key):
        return self.contains(key)

    def __add__(self, other):
        self.add(other)
        return self

    def __len__(self):
        return self.get_size()
