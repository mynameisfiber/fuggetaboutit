# Fugget About It
[![Build Status](https://secure.travis-ci.org/mynameisfiber/fuggetaboutit.png?branch=master)](http://travis-ci.org/mynameisfiber/fuggetaboutit)
[![PyPI version](https://badge.fury.io/py/fuggetaboutit.svg)](https://badge.fury.io/py/fuggetaboutit)

> auto-scaling probabilistic time windowed set inclusion datastructure

[_docs_](http://micha.codes/fuggetaboutit)

### what is?

What does this mean?  Well... it means you can have a rolling window view on
unique items in a stream (using the `TimingBloomFilter` object) and also have
it rescale itself when the number of unique items increases beyond what you had
anticipated (using the `ScalingTimingBloomFilter`).  And, since this is built
on bloom filters, the number of bits per entry is generally EXCEEDINGLY small
letting you keep track of many items using a small amount of resources while
still having very tight bounds on error.

So, let's say you have a stream coming in 24 hours a day, 7 days a week.  This
stream contains phone numbers and you want to ask the question "Have I seen
this phone number in the past day?".  This could be answered with the following
code stub:

```
from fuggetaboutit import TimingBloomFilter

cache = TimingBloomFilter(capacity=1000000, decay_time=24*60*60).start()

def handle_message(phone_number):
    if phonenumber in cache:
        print "I have seen this before: ", phone_number
    cache.add(phone_number)
```

Assuming you have a `tornado.ioloop` running, this will automatically forget
old values for you and only print if the phone number has been seen *in the
last 24hours*.  (NOTE: If you do not have an IOLoop running, don't worry...
just call the `TimingBloomFilter.decay()` method every half a decay interval or
every 12 hours in this example).

Now, this example assumed you had apriori knowledge about how many unique phone
numbers you would expect -- we told fuggetaboutit that we would have at most
1000000 unique phone numbers.  What happens if we don't know this number
beforehand or we know that this value varies wildly?  In this case, we can use
the `ScalingTimingBloomFilter`

```
from fuggetaboutit import ScalingTimingBloomFilter

cache = ScalingTimingBloomFilter(capacity=1000000, decay_time=24*60*60).start()

def handle_message(phone_number):
    if phonenumber in cache:
        print "I have seen this before: ", phone_number
    cache.add(phone_number)
```

This will automatically build new bloom filters as needed, and delete unused
one.  In this case, the capacity is simply a baseline capacity and we can
easily grow beyond it.

### speed

Did we mention that this thing is fast?  It's all built on numpy ndarray's and
uses a c-python module to optimize all of the important bits.  On a 2011
MacBook Air, I get:

```
$ python -m fuggetaboutit.benchmark
Benchmarking blooms with size 100000
(baseline timing of keygeneration: 9.84e-06s, already subtracted from results)
.-------------------------------------------------------------------------------.
|                                    | bench_add | bench_contains | bench_decay |
|===============================================================================|
|                Timing Bloom Filter | 1.09e-05s | 8.1764627e-06s | 1.9898e-03s |
|        Scaling Timing Bloom Filter | 1.57e-05s | 1.6510360e-05s | 2.3653e-03s |
| Scaled Scaling Timing Bloom Filter | 2.41e-05s | 1.9161074e-05s | 1.5937e-02s |
'-------------------------------------------------------------------------------'
```

For these benchmarks, the first and second entries are empty
`TimingBloomFilter` and `ScalingTimingBloomFilter` objects with capacity
100000.  The same is the case for the last entry, however we also added 150000
entries before the test so that the bloom is in a scaled state.

### todo

**MOAR SPEED**


### References

Fuggetaboutit was inspired by the following papers

* Paulo Sérgio Almeida, Carlos Baquero, Nuno Preguiça, David Hutchison;
  ["Scalable Bloom Filters"](http://asc.di.fct.unl.pt/~nmp/pubs/ref--04.pdf)
* Jonathan L. Dautrich, Chinya V. Ravishankar; ["Inferential Time-Decaying
  Bloom Filters"
  ](http://www.edbt.org/Proceedings/2013-Genova/papers/edbt/a23-dautrich.pdf)
* Adam Kirsch, Michael Mitzenmacher; ["Less Hashing, Same Performance: Building
  a Better Bloom
  Filter"](http://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf)
