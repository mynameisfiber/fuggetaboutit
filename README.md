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

**ScalingTimingBloomFilter**:
* Adding 200000 values: 5.104190s (39183.495322 / s)
* Testing 200000 positive values: 7.245727s (27602.475896 / s)
* Testing 200000 negative values: 5.937196s (33685.935154 / s)
* Deaying: between 2.058376s - 0.606063s depending on state


**TimingBloomFilter**:
* Adding 100000 values: 2.077839s (48126.926544 / s)
* Testing 100000 positive values: 2.268268s (44086.503770 / s)
* Testing 100000 negative values: 0.982043s (101828.532112 / s)
* Decaying: 0.966145s

### todo

**MOAR SPEED**
