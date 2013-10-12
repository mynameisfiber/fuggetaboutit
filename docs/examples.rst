Examples
========

Basic Usage
###########

Let's say we have the following data source::

    import random
    import string

    def generate_data(N=5):
        while True:
            yield "".join(random.sample(string.ascii_lowercase, N))

and we would like to know, "**Have we seen this piece of data in the last 10
seconds?**" we could do the following::

    from fuggetaboutit import ScalingTimingBloomFilter
    import time

    decay_time = 10
    cache = ScalingTimingBloomFilter(20000, decay_time=decay_time)
    for data in generate_data():
        if data in cache:
            print "I have seen this before: ", data
        cache.add(data)
        cache.decay()

We picked ``20000`` as an initial guess for how many unique items would be
stored in the bloom at any given time.  Since this bloom filter scales, this
initial guess can be wrong and the bloom will scale as necissary (at a
performance penalty!).

.. note::

    Decaying is not necissary if the capacity is very close to the number of
    unique elements in the set in one decay_time.  This is because new inserts
    will overwite old data with new timestamps and the old data will simply
    fade away.  However, it is always best practive to do an explicit decay to
    make sure we satisfy the guarentee of decaying data and the given erro rate
    even in the face of edge cases.

One problem with this code is that we are decaying the entire bloom for every
input we get.  This is a waste of computation since decaying is only necissary
at every half decay time.  In order to deal with this, we can keep a timer and
only decay when necissary::

    from fuggetaboutit import ScalingTimingBloomFilter
    import time

    decay_time = 10
    cache = ScalingTimingBloomFilter(20000, decay_time=decay_time)
    last_decay = time.time()
    for data in generate_data():
        if data in cache:
            print "I have seen this before: ", data
        cache.add(data)
        if time.time() - last_decay >= decay_time:
            cache.decay()
            last_decay = time.time()

Unfortunaly, there is quite a lot of code dealing with when to run a ``decay``.
For this reason, the ``ScalingTimingBloomFilter`` supports tornado's IOLoop.

Using the IOLoop
################

In order to use the IOLoop, we must re-make the code to be event based::

    import tornado.ioloop
    from fuggetaboutit import ScalingTimingBloomFilter

    data = generate_data()
    cache = ScalingTimingBloomFilter(20000, decay_time=decay_time)

    def handle_data(d):
        if d in cache:
            print "I have seen this before: ", d
        cache.add(d)

    def generate_event():
        for i in xrange(500):
            handle_data(data.next())

    cache.start()
    tornado.ioloop.PeriodicCallback(generate_event, 500).start()

This will generate 500 data event every 0.5 seconds.  Similarly, the
``.start()`` call on the ``ScalingTimingBloomFilter`` registers decay events
with the IOLoop.  This produces the final code::

    from fuggetaboutit import ScalingTimingBloomFilter
    import tornado.ioloop
    import random
    import string

    def generate_data(N=5):
        while True:
            yield "".join(random.sample(string.ascii_lowercase, N))

    data = generate_data()
    cache = ScalingTimingBloomFilter(20000, decay_time=10)

    def handle_data(d):
        if d in cache:
            print "I have seen this before: ", d
        cache.add(d)

    def generate_event():
        for i in xrange(500):
            handle_data(data.next())

    if __name__ == "__main__":
        tornado.ioloop.PeriodicCallback(generate_event, 100).start()
        cache.start()
        print "Starting"
        tornado.ioloop.IOLoop().instance().start()

With this type of setup, we can asynchronously add data and decay old events.
