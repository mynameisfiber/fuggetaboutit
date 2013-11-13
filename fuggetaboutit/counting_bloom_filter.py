#!/usr/bin/env python

import numpy as np
import struct
import math
import mmh3


class CountingBloomFilter(object):
    _ENTRIES_PER_8BYTE = 1
    def __init__(self, capacity, error=0.005):
        self.capacity = capacity
        self.error = error
        self.num_bytes = int(-capacity * math.log(error) / math.log(2)**2) + 1
        self.num_hashes = int(self.num_bytes / capacity * math.log(2)) + 1

        size = int(math.ceil(self.num_bytes / self._ENTRIES_PER_8BYTE))
        self.data = np.zeros((size,), dtype=np.uint8, order='C')

    def _indexes(self, key):
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
        for index in self._indexes(key):
            self.data[index] += N
        return self

    def remove(self, key, N=1):
        """
        Removes `N` counts to the indicies given by the key
        """
        assert isinstance(key, str)
        indexes = list(self._indexes(key))
        if not any(self.data[index] < N for index in indexes):
            for index in indexes:
                self.data[index] -= N
        return self

    def remove_all(self, N=1):
        """
        Removes `N` counts to all indicies.  Useful for expirations
        """
        for i in xrange(self.num_bytes):
            if self.data[i] >= N:
                self.data[i] -= N

    def contains(self, key):
        """
        Check if the current bloom contains the key `key`
        """
        assert isinstance(key, str)
        return all(self.data[index] != 0 for index in self._indexes(key))

    def size(self):
        return -self.num_bytes * math.log(1 - self.num_non_zero / float(self.num_bytes)) / float(self.num_hashes) 

    COUNTING_HEADER = "QdQQ"
    def tofile(self, f):
        """
        Writes the bloom into the given fileobject.
        """
        header = struct.pack(self.COUNTING_HEADER, self.capacity, self.error, self.num_bytes, self.num_hashes)
        f.write(header)
        self.data.tofile(f)

    @classmethod
    def fromfile(cls, f):
        """
        Reads the bloom from the given fileobject and returns the python object
        """
        self = cls.__new__(cls)
        sizeof_header = struct.calcsize(cls.COUNTING_HEADER)
        header = f.read(sizeof_header)
        self.capacity, self.error, self.num_bytes, self.num_hashes = struct.unpack(cls.COUNTING_HEADER, header)
        self.data = np.fromfile(f, dtype=np.uint8, count=self.num_bytes)
        return self

    def __contains__(self, key):
        return self.contains(key)

    def __add__(self, other):
        return self.add(other)

    def __sub__(self, other):
        return self.remove(other)

    def __len__(self):
        return self.size()


