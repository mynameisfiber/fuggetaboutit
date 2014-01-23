import logging
import os
import json
import shutil

import numpy as np
import math
import mmh3

from .exceptions import PersistenceDisabledException


BLOOM_FILENAME = 'bloom.npy'
META_FILENAME = 'meta.json'

def remove_recursive(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.exists(path):
        os.remove(path)

class CountingBloomFilter(object):
    _ENTRIES_PER_8BYTE = 1
    def __init__(self, capacity, data_path=None, error=0.005, id=None):
        self.capacity = capacity
        self.error = error
        self.data_path = data_path
        self.id = id

        self.num_bytes = int(-capacity * math.log(error) / math.log(2)**2) + 1
        self.num_hashes = int(self.num_bytes / capacity * math.log(2)) + 1

        bloom_filename = None

        if data_path:
            data_path = os.path.normpath(data_path)
            bloom_filename = os.path.join(data_path, BLOOM_FILENAME)

        if bloom_filename and os.path.exists(bloom_filename):
            self.data = np.load(bloom_filename)
            self.num_non_zero = np.count_nonzero(self.data)
        else:
            size = int(math.ceil(self.num_bytes / float(self._ENTRIES_PER_8BYTE)))
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

    def get_meta(self):
        return {
            'capacity': self.capacity,
            'error': self.error,
            'id': self.id,
        }

    def flush_data(self, data_path=None):
        _, _, bloom_path = self._get_paths(data_path)
        tmp_bloom_path = bloom_path + ".tmp"

        self._save_data(tmp_bloom_path)
        os.rename(tmp_bloom_path, bloom_path)

    def save(self, data_path=None):
        data_path, meta_path, bloom_path = self._get_paths(data_path)
        tmp_data_path, tmp_meta_path, tmp_bloom_path = self._get_paths(data_path + '-tmp')

        remove_recursive(tmp_data_path)
        os.makedirs(tmp_data_path)

        self._save_meta(tmp_meta_path)
        self._save_data(tmp_bloom_path)

        remove_recursive(data_path)
        os.rename(tmp_data_path, data_path)

    def _get_paths(self, data_path):
        if not (data_path or self.data_path):
            raise PersistenceDisabledException("You cannot save without having data_path set.")
        if not data_path:
            data_path = self.data_path

        data_path = os.path.normpath(data_path)
        meta_path = os.path.join(data_path, META_FILENAME)
        bloom_path = os.path.join(data_path, BLOOM_FILENAME)
        return data_path, meta_path, bloom_path

    def _save_meta(self, filename):
        with open(filename, 'w') as meta_file:
            json.dump(self.get_meta(), meta_file)

    def _save_data(self, filename):
        np.save(filename, self.data)


    @classmethod
    def load(cls, data_path):
        logging.info("Loading counting bloom from %s" % data_path)
        kwargs = None

        with open(os.path.join(data_path, META_FILENAME), 'r') as meta_file:
            kwargs = json.load(meta_file)

        kwargs['data_path'] = data_path

        capacity = kwargs['capacity']
        del kwargs['capacity']

        return cls(capacity, **kwargs)


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
