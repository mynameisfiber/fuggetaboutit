from .test_scaling_timing_bloom_filter import TestScalingTimingBloomFilter
from .test_timing_bloom_filter import TestTimingBloomFilter

def all():
    return TestTimingBloomFilter, TestScalingTimingBloomFilter
