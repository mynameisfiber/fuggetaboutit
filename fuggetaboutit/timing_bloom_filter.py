import logging 
import time
import json
import os

from .counting_bloom_filter import CountingBloomFilter
from . import _optimizations

_ENTRIES_PER_8BYTE = 2 if _optimizations is not None else 1

META_FILENAME = 'meta.json'

class TimingBloomFilter(CountingBloomFilter):
    _ENTRIES_PER_8BYTE = _ENTRIES_PER_8BYTE

    def __init__(self, capacity, decay_time, disable_optimizations=False, *args, **kwargs):
        self.decay_time = decay_time
        if disable_optimizations:
            self._optimize = False
            self._ENTRIES_PER_8BYTE = 1
        else:
            self._optimize = _optimizations is not None

        super(TimingBloomFilter, self).__init__(capacity, *args, **kwargs)

        self.ring_size = (1 << (8 / self._ENTRIES_PER_8BYTE)) - 1
        self.dN = self.ring_size / 2
        self.seconds_per_tick = self.decay_time / float(self.dN)


    def get_tick(self, timestamp=None):
        return int(((timestamp or time.time()) // self.seconds_per_tick) % self.ring_size) + 1

    def get_tick_range(self):
        tick_max = self.get_tick()
        tick_min = (tick_max - self.dN - 1) % self.ring_size + 1
        return tick_min, tick_max

    def get_interval_test(self):
        tick_min, tick_max = self.get_tick_range()

        if tick_min < tick_max:
            return lambda x : x and tick_min < x <= tick_max
        else:
            return lambda x : x != 0 and not tick_max < x <= tick_min

    def add(self, key, timestamp=None):
        tick = self.get_tick(timestamp)
        if timestamp:
            if timestamp < time.time() - self.decay_time:
                return
        if self._optimize and self.data.flags['C_CONTIGUOUS']:
            self.num_non_zero += _optimizations.timing_bloom_add(self.data, self.get_indexes(key), tick)
        else:
            for index in self.get_indexes(key):
                self.num_non_zero += (self.data[index] == 0)
                self.data[index] = tick

    def contains(self, key):
        """
        Check if the current bloom contains the key `key`
        """
        if self._optimize and self.data.flags['C_CONTIGUOUS']:
            tick_min, tick_max = self.get_tick_range()
            return bool(_optimizations.timing_bloom_contains(self.data, self.get_indexes(key), tick_min, tick_max))
        else:
            test_interval = self.get_interval_test()
            return all(test_interval(self.data[index]) for index in self.get_indexes(key))

    def decay(self):
        if self._optimize and self.data.flags['C_CONTIGUOUS']:
            logging.info("Starting optimized decay")
            tick_min, tick_max = self.get_tick_range()
            self.num_non_zero = _optimizations.timing_bloom_decay(self.data, tick_min, tick_max)
            logging.info("Optimized decay finished")
        else:
            logging.info("Starting un-optimized decay")
            test_interval = self.get_interval_test()
            self.num_non_zero = 0
            for i in xrange(self.num_bytes):
                value = self.data[i]
                if value != 0:
                    if not test_interval(value):
                        self.data[i] = 0
                    else:
                        self.num_non_zero += 1
            logging.info("Un-optimized decay finished")

    def get_meta(self):
        meta = super(TimingBloomFilter, self).get_meta()
        meta['decay_time'] = self.decay_time
        meta['disable_optimizations'] = not self._optimize
        return meta

    def remove(self, *args, **kwargs):
        raise NotImplementedError

    def remove_all(self, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def load(cls, data_path):
        logging.info("Loading timing bloom from %s" % data_path)
        kwargs = None

        with open(os.path.join(data_path, META_FILENAME), 'r') as meta_file:
            kwargs = json.load(meta_file)

        kwargs['data_path'] = data_path

        capacity = kwargs['capacity']
        del kwargs['capacity']

        decay_time = kwargs['decay_time']
        del kwargs['decay_time']

        return cls(capacity, decay_time, **kwargs)
