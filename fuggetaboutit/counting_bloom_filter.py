import logging
import os
import json

import numpy as np
import math
import mmh3

from .exceptions import PersistenceDisabledException


class CountingBloomFilter(object):
    _ENTRIES_PER_8BYTE = 1
    def __init__(self, capacity, data_path=None, error=0.005, id=None):
        self.capacity = capacity
        self.error = error
        self.data_path = data_path

        self.num_bytes = int(-capacity * math.log(error) / math.log(2)**2) + 1
        self.num_hashes = int(self.num_bytes / capacity * math.log(2)) + 1
        
        if data_path:
            self.bloom_filename = os.path.join(data_path, 'bloom.npy')
            self.meta_filename = os.path.join(data_path, 'meta.json')
        else:
            self.bloom_filename = None
            self.meta_filename = None
        
        self.id = id

        if self.bloom_filename and os.path.exists(self.bloom_filename):
            self.data = np.load(self.bloom_filename)
            self.num_non_zero = np.count_nonzero(self.data)
        else:
            size = int(math.ceil(self.num_bytes / self._ENTRIES_PER_8BYTE))
            self.data = np.zeros((size,), dtype=np.uint8, order='C')
            self.num_non_zero = 0

    def get_indexes(self, key):
        """
        Generates the indicies corresponding to the given key
        """
        h1, h2 = mmh3.hash64(key)
        for i in xrange(self.num_hashes):
            yield (h1 + i * h2) % self.num_bytes

    def add(self, key, N=1):
        """
        Adds `N` counts to the indicies given by the key
        """
        assert isinstance(key, str)
        for index in self.get_indexes(key):
            self.num_non_zero += (self.data[index] == 0)
            self.data[index] += N

    def decrement_bucket(self, index, N=1):
        """
        Safely and efficiently decrements the value of a given bucket
        """
        data = self.data
        old_value = data[index]
        new_value = 0

        if old_value == 0:
            return 0

        if old_value <= N:
            new_value = 0
            self.num_non_zero -= 1
        else:
            new_value = old_value - N

        data[index] = new_value
        return new_value

    def remove(self, key, N=1):
        """
        Removes `N` counts to the indicies given by the key
        """
        assert isinstance(key, str)
        indexes = list(self.get_indexes(key))
        for index in indexes:
            self.decrement_bucket(index, N)

    def remove_all(self, N=1):
        """
        Removes `N` counts to all indicies.  Useful for expirations
        """
        for i in xrange(self.num_bytes):
            self.decrement_bucket(i, N)

    def contains(self, key):
        """
        Check if the current bloom contains the key `key`
        """
        assert isinstance(key, str)
        return all(self.data[index] != 0 for index in self.get_indexes(key))

    def get_size(self):
        """
        Returns the density of the bloom which can be used to determine if the bloom is "full"
        """
        return -self.num_bytes * math.log(1 - self.num_non_zero / float(self.num_bytes)) / float(self.num_hashes) 

    def can_persist(self):
        return self.data_path is not None

    def flush_data(self):
        if not self.can_persist():
            raise PersistenceDisabledException("You cannot flush data without having data_path set.")

        np.save(self.bloom_filename, self.data)

    def get_meta(self):
        return {
            'capacity': self.capacity,
            'error': self.error,
            'id': self.id,
        }

    def save(self):
        if not self.can_persist():
            raise PersistenceDisabledException("You cannot save without having data_path set.")

        logging.info("Saving counting bloom to %s" % self.data_path)
        if not os.path.exists(self.data_path):
            logging.info("Bloom path doesn't exist, creating:  %s" % self.data_path)
            os.makedirs(self.data_path)

        self.flush_data()
        meta = self.get_meta()

        with open(self.meta_filename, 'w') as meta_file:
            json.dump(meta, meta_file)

    @classmethod
    def load(cls, data_path):
        logging.info("Loading counting bloom from %s" % data_path)
        kwargs = None

        with open(os.path.join(data_path, 'meta.json'), 'r') as meta_file:
            kwargs = json.load(meta_file)

        kwargs['data_path'] = data_path

        return cls(**kwargs)


    def __contains__(self, key):
        return self.contains(key)

    def __add__(self, other):
        self.add(other)
        return self

    def __sub__(self, other):
        self.remove(other)
        return self

    def __len__(self):
        return self.get_size()


