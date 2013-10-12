# Fugget About It
[![Build Status](https://secure.travis-ci.org/mynameisfiber/fuggetaboutit.png?branch=master)](http://travis-ci.org/mynameisfiber/fuggetaboutit)

> auto-scaling probabilistic time windowed set inclusion datastructure

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

Did we mention that this thing is fast?  It's all built on python's native
`array` module and tries as hard as it can to be performant.  On a 2011 MacBook
Air, I get:

```
$ ipython
Python 2.7.1 (r271:86832, Jul 31 2011, 19:30:53)
Type "copyright", "credits" or "license" for more information.

IPython 0.14.dev -- An enhanced Interactive Python.
In [1]: import random, string

In [2]: from fuggetaboutit import TimingBloomFilter

In [3]: from fuggetaboutit import ScalingTimingBloomFilter

In [4]: tbf = TimingBloomFilter(1e6, decay_time=24*60*60)

In [5]: %timeit "".join(random.sample(string.ascii_lowercase, 5))
10000 loops, best of 3: 22.7 us per loop

In [6]: %timeit tbf.add("".join(random.sample(string.ascii_lowercase, 5)))
10000 loops, best of 3: 60 us per loop

In [7]: %timeit tbf.contains("".join(random.sample(string.ascii_lowercase, 5)))
10000 loops, best of 3: 43.3 us per loop

In [8]: %timeit tbf.decay()
10 loops, best of 3: 153 ms per loop

In [9]: stbf = ScalingTimingBloomFilter(1e6, decay_time=24*60*60)

In [10]: %timeit stbf.add("".join(random.sample(string.ascii_lowercase, 5)))
10000 loops, best of 3: 62.5 us per loop

In [11]: %timeit stbf.contains("".join(random.sample(string.ascii_lowercase, 5)))
10000 loops, best of 3: 54.3 us per loop

In [12]: %timeit stbf.decay()
10 loops, best of 3: 143 ms per loop

#After loading the stbf to more contain more entries than it's initial capacity
In [13]: for i in xrange(int(2e6)): stbf.add("".join(random.sample(string.ascii_lowercase, 10)))

In [14]: %timeit stbf.add("".join(random.sample(string.ascii_lowercase, 5)))
10000 loops, best of 3: 65.3 us per loop

In [15]: %timeit stbf.contains("".join(random.sample(string.ascii_lowercase, 5)))
10000 loops, best of 3: 67.6 us per loop

In [16]: %timeit stbf.decay()
1 loops, best of 3: 597 ms per loop
```


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
