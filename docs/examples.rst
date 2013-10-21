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


Scaling Bloom Size Convergence
##############################

Scaling blooms offer the ability to grow when more capacity is necissary.
Since timing blooms also decay out data, we should be able to also reduce the
size of the blooms that are in use and hopefully stabalize to a desireable
size.

The `min_fill_factor` and `max_fill_factor` do exactly that.  By using an
estimate for the number of elements current in a bloom, we find the fill ratio
(estimated number of items / capacity of the bloom) and scale up or down
depending on whether we are above/below the max or min fill ratios.

For example, let's say we initialize a bloom with a capacity for 30 items and a
decay time of 60 seconds.  If we started inserting 45 items per second, we
should hope the timing bloom would settle with a single bloom with a capacity
for 60 items instead of maintaining multiple blooms that could potentially
hinder performance::

    import time
    
    cache = ScalingTimingBloomFilter(
        30, 
        decay_time=60, 
        max_fill_factor=0.9, 
        min_fill_factor=0.2, 
        growth_factor=2
    ).start()
    for item in generate_data(N=10):
        # insert at 1 item per 1.2second for 50 items per minute
        cache.add(item)
        time.sleep(1.2) 

After about 27 seconds, we have inserted 90% of the capacity into the bloom and
it gets scaled up.  By default, the next bloom filter will have `2 * sqrt(2)`
the capacity (this value is controlled by `growth_factor`).  This new bloom
then becomes the preferential bloom for inserts.  After 60 seconds from this
point, all the data in the original bloom will have been decayed out and that
bloom will be deleted.  This leave the `ScalingTimingBloomFilter` with only one
operational bloom with a larger capacity.

If the rate of inserts starts decreasing, then we now have a larger bloom
filter than is necissary for the problem.  If the rate decreases down such that
there are only 12 items in the new bloom of capacity 62, then we will scale it
down.  We do this by creating a new bloom with half the capacity and keeping it
as the preferential bloom for insertions.  After some time, the old bloom will
decay and get deleted.

Thus, by tuning the parameters for `min_fill_factor`, `max_fill_factor` and
`growth_factor` (keeping in mind the desired `decay_time` and the rate of
insertion of data), we can have the bloom converge on the proper capacity and
operate using one underlying bloom filter.
