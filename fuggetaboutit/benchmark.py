#!/usr/bin/env python

from fuggetaboutit import ScalingTimingBloomFilter, TimingBloomFilter

import time
import random
import string
from functools import wraps

def time_benchmark(fxn):
    @wraps(fxn)
    def _(*args, **kwargs):
        start = time.time()
        fxn(*args, **kwargs)
        return time.time() - start
    return _

def key(N=5):
    return "".join(random.sample(string.ascii_lowercase, N))

@time_benchmark
def bench_keygen(N):
    for i in xrange(N):
        t = key()

@time_benchmark
def bench_add(bloom, N):
    for i in xrange(N):
        bloom.add(key())

@time_benchmark
def bench_contains(bloom, N):
    for i in xrange(N):
        t = key() in bloom

@time_benchmark
def bench_decay(bloom, N):
    for i in xrange(N):
        bloom.decay()

def print_benchmark(b):
    print "Benchmarking blooms with size %d" % b["metadata"]["N"]
    print "(baseline timing of keygeneration: %.2es, already subtracted from results)" % b["metadata"]["baseline"]

    test_names = [t["fxn"].func_name for t in b["metadata"]["tests"]]
    l = "| %s | " % (" "*34) + " | ".join(test_names) + " |"
    print '.' + ("-" * (len(l)-2)) + '.'
    print l
    print '|' + ("=" * (len(l)-2)) + '|'
    for result in b["data"]:
        print ("| %34s |" % result["name"]),
        for t in test_names:
            n = len(t) - 7
            print ("%%.%des" % n) % result["result"][t], "|",
        print ""
    print "'" + ("-" * (len(l)-2)) + "'"



def benchmark():
    N = int(1e5)
    n = int(1e4)
    d = 10
    stbf = ScalingTimingBloomFilter(N, decay_time=d)
    sstbf = ScalingTimingBloomFilter(N, decay_time=d)
    tbf  = TimingBloomFilter(N, decay_time=d)

    # Add a bunch of values so that this bloom has to scale up
    for i in xrange(int(N*1.5)):
        sstbf.add(key())

    baseline = bench_keygen(N) / float(N)
    tests = [
        {
            "fxn" : bench_add,
            "N" : n,
        },
        {
            "fxn" : bench_contains,
            "N" : n,
        },
        {
            "fxn" : bench_decay,
            "N" : 10,
        },
    ]
    results = {
        "data" : [
            {
                "name" : "Timing Bloom Filter",
                "object" : tbf,
            },
            {
                "name" : "Scaling Timing Bloom Filter",
                "object" : stbf,
            },
            {
                "name" : "Scaled Scaling Timing Bloom Filter",
                "object" : sstbf,
            },
        ],
        "metadata" : {
            "baseline" : baseline,
            "N" : N,
            "d" : d,
            "tests" : tests
        }
    }

    for item in results["data"]:
        result = {}
        b = item["object"]
        for test in tests:
            name = test["fxn"].func_name
            n = test["N"]
            value = test["fxn"](b, n) / float(n)
            result[name] = value - baseline
        item["result"] = result

    return results

if __name__ == "__main__":
    result = benchmark()
    print_benchmark(result)
