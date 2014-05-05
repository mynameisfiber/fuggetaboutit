from copy import copy
import time
import random

from fuggetaboutit.scaling_timing_bloom_filter import ScalingTimingBloomFilter


BLOOM_DEFAULTS = {
    'capacity': 1000,
    'decay_time': 86400,
}

def get_bloom( n=100, **updates):
    kwargs = copy(BLOOM_DEFAULTS)
    kwargs.update(updates)

    # Create a bloom
    bloom = ScalingTimingBloomFilter(**kwargs)

    # Add a bunch of keys
    for i in range(100):
        bloom.add(str(i))

    # Check that the bloom is working as expected
    assert bloom.contains('1')
    assert bloom.contains(str(n // 2))
    assert not bloom.contains(str(n + 1))

    return bloom

def get_pseudorandom_words(num_words=1000, word_length=12):
    random.seed(1234)
    all_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvqxyz"
    result_list = []
    for _ in xrange(num_words):
        word = "".join(random.choice(all_letters) for _ in range(word_length))
        result_list.append(word)
    return result_list


def test_bloom_initial_save_and_load_with_optimization(tmpdir):
    testing_dir = tmpdir.mkdir('bloom_test')
    # Setup a temporary directory
    temp_path = str(testing_dir)
    # Get a bloom for testing 
    bloom = get_bloom(data_path=temp_path)

    # Save the bloom
    bloom.save()

    # Check that the expected files were created
    blooms_path = testing_dir.join('blooms')
    meta_file = testing_dir.join('meta.json')
    assert blooms_path.check()
    assert 1 == len(blooms_path.listdir())
    assert meta_file.check()

    # Reload the bloom
    reloaded = ScalingTimingBloomFilter.load(temp_path)

    # Check that the reloaded bloom is working as expected
    assert reloaded.contains('1')
    assert reloaded.contains('50')
    assert not reloaded.contains('101')


def test_bloom_repeat_saves_with_optimization(tmpdir):
    testing_dir = tmpdir.mkdir('bloom_test')
    # Setup a temporary directory
    temp_path = str(testing_dir)
    # Get a bloom for testing 
    bloom = get_bloom(data_path=temp_path)

    # Save the bloom
    bloom.save()

    # Capture the mtime on the save files
    bloom_file = testing_dir.join('blooms/0/bloom.npy')
    meta_file = testing_dir.join('meta.json')
    first_bloom_save = bloom_file.mtime()
    first_meta_save = meta_file.mtime()

    # Sleep for 1 second to deal ensure mtimes will advance
    time.sleep(1)

    # Reload the bloom
    second_gen_bloom = ScalingTimingBloomFilter.load(temp_path)

    # Add a few more keys to the reloaded bloom
    second_gen_bloom.add('101')
    second_gen_bloom.add('102')
    second_gen_bloom.add('103')

    # Save the second generation bloom
    second_gen_bloom.save()

    # Check that the mtimes have changed
    assert first_bloom_save <  bloom_file.mtime()
    assert first_meta_save < meta_file.mtime()

    # Load the bloom one more time
    third_gen_bloom = ScalingTimingBloomFilter.load(temp_path)

    # Check that the loaded data is as expected
    assert third_gen_bloom.contains('50')
    assert third_gen_bloom.contains('103')
    assert not third_gen_bloom.contains('105')


def test_bloom_initial_save_and_load_without_optimization(tmpdir):
    testing_dir = tmpdir.mkdir('bloom_test')
    # Setup a temporary directory
    temp_path = str(testing_dir)
    # Get a bloom for testing 
    bloom = get_bloom(data_path=temp_path, disable_optimizations=True)

    # Save the bloom
    bloom.save()

    # Check that the expected files were created
    blooms_path = testing_dir.join('blooms')
    meta_file = testing_dir.join('meta.json')
    assert blooms_path.check()
    assert 1 == len(blooms_path.listdir())
    assert meta_file.check()

    # Reload the bloom
    reloaded = ScalingTimingBloomFilter.load(temp_path)

    # Check that the reloaded bloom is working as expected
    assert reloaded.contains('1')
    assert reloaded.contains('50')
    assert not reloaded.contains('101')


def test_bloom_repeat_saves_without_optimization(tmpdir):
    testing_dir = tmpdir.mkdir('bloom_test')
    # Setup a temporary directory
    temp_path = str(testing_dir)
    # Get a bloom for testing 
    bloom = get_bloom(data_path=temp_path, disable_optimizations=True)

    # Save the bloom
    bloom.save()

    # Capture the mtime on the save files
    bloom_file = testing_dir.join('blooms/0/bloom.npy')
    meta_file = testing_dir.join('meta.json')
    first_bloom_save = bloom_file.mtime()
    first_meta_save = meta_file.mtime()

    # Sleep for 1 second to deal ensure mtimes will advance
    time.sleep(1)

    # Reload the bloom
    second_gen_bloom = ScalingTimingBloomFilter.load(temp_path)

    # Add a few more keys to the reloaded bloom
    second_gen_bloom.add('101')
    second_gen_bloom.add('102')
    second_gen_bloom.add('103')

    # Save the second generation bloom
    second_gen_bloom.save()

    # Check that the mtimes have changed
    assert first_bloom_save <  bloom_file.mtime()
    assert first_meta_save < meta_file.mtime()

    # Load the bloom one more time
    third_gen_bloom = ScalingTimingBloomFilter.load(temp_path)

    # Check that the loaded data is as expected
    assert third_gen_bloom.contains('50')
    assert third_gen_bloom.contains('103')
    assert not third_gen_bloom.contains('105')


def test_save_and_load_with_scaling(tmpdir):
    testing_dir = tmpdir.mkdir('bloom_test')
    # Setup a temporary directory
    temp_path = str(testing_dir)

    # Get a bloom for testing 
    bloom = get_bloom(data_path=temp_path, disable_optimizations=True, capacity=200)

    # Save the bloom
    bloom.save()

    # Check that the expected files were created
    blooms_path = testing_dir.join('blooms')
    meta_file = testing_dir.join('meta.json')
    assert blooms_path.check()
    assert 1 == len(blooms_path.listdir())
    assert meta_file.check()

    # Reload the bloom
    second_gen_bloom = ScalingTimingBloomFilter.load(temp_path)

    # Add enough items to trigger a scale
    for i in range(101, 201):
        second_gen_bloom.add(str(i))

    # Call save
    second_gen_bloom.save()

    # Check that the expected files were created
    blooms_path = testing_dir.join('blooms')
    meta_file = testing_dir.join('meta.json')
    assert blooms_path.check()
    assert 2 == len(blooms_path.listdir())
    assert meta_file.check()

    # Load again and make sure the new keys are found
    third_gen_bloom = ScalingTimingBloomFilter.load(temp_path)

    assert third_gen_bloom.contains('101')
    assert third_gen_bloom.contains('150')
    assert not third_gen_bloom.contains('201')


def test_scaling_bloom_accuracy(tmpdir):
    testing_dir = tmpdir.mkdir('bloom_test')
    temp_path = str(testing_dir)

    test_words = get_pseudorandom_words(num_words=9000)
