import time

from fuggetaboutit.timing_bloom_filter import TimingBloomFilter


def get_bloom(temp_path, disable_optimizations=False):

    # Create a bloom
    bloom = TimingBloomFilter(
        capacity=1000,
        decay_time=86400,
        data_path=temp_path,
        disable_optimizations=disable_optimizations,
    )

    # Add a bunch of keys
    for i in range(100):
        bloom.add(str(i))

    # Check that the bloom is working as expected
    assert bloom.contains('1')
    assert bloom.contains('50')
    assert not bloom.contains('101')

    return bloom


def test_bloom_initial_save_and_load_with_optimization(tmpdir):
    testing_dir = tmpdir.mkdir('bloom_test')
    # Setup a temporary directory
    temp_path = str(testing_dir)
    # Get a bloom for testing 
    bloom = get_bloom(temp_path)

    # Save the bloom
    bloom.save()

    # Check that the expected files were created
    bloom_file = testing_dir.join('bloom.npy')
    meta_file = testing_dir.join('meta.json')
    assert bloom_file.check()
    assert meta_file.check()

    # Reload the bloom
    reloaded = TimingBloomFilter.load(temp_path)

    # Check that the reloaded bloom is working as expected
    assert reloaded.contains('1')
    assert reloaded.contains('50')
    assert not reloaded.contains('101')


def test_bloom_repeat_saves_with_optimization(tmpdir):
    testing_dir = tmpdir.mkdir('bloom_test')
    # Setup a temporary directory
    temp_path = str(testing_dir)
    # Get a bloom for testing 
    bloom = get_bloom(temp_path)

    # Save the bloom
    bloom.save()

    # Capture the mtime on the save files
    bloom_file = testing_dir.join('bloom.npy')
    meta_file = testing_dir.join('meta.json')
    first_bloom_save = bloom_file.mtime()
    first_meta_save = meta_file.mtime()

    # Sleep for 1 second to deal ensure mtimes will advance
    time.sleep(1)

    # Reload the bloom
    second_gen_bloom = TimingBloomFilter.load(temp_path)

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
    third_gen_bloom = TimingBloomFilter.load(temp_path)

    # Check that the loaded data is as expected
    assert third_gen_bloom.contains('50')
    assert third_gen_bloom.contains('103')
    assert not third_gen_bloom.contains('105')


def test_bloom_initial_save_and_load_without_optimization(tmpdir):
    testing_dir = tmpdir.mkdir('bloom_test')
    # Setup a temporary directory
    temp_path = str(testing_dir)
    # Get a bloom for testing 
    bloom = get_bloom(temp_path, disable_optimizations=True)

    # Save the bloom
    bloom.save()

    # Check that the expected files were created
    bloom_file = testing_dir.join('bloom.npy')
    meta_file = testing_dir.join('meta.json')
    assert bloom_file.check()
    assert meta_file.check()

    # Reload the bloom
    reloaded = TimingBloomFilter.load(temp_path)

    # Check that the reloaded bloom is working as expected
    assert reloaded.contains('1')
    assert reloaded.contains('50')
    assert not reloaded.contains('101')


def test_bloom_repeat_saves_without_optimization(tmpdir):
    testing_dir = tmpdir.mkdir('bloom_test')
    # Setup a temporary directory
    temp_path = str(testing_dir)
    # Get a bloom for testing 
    bloom = get_bloom(temp_path, disable_optimizations=True)

    # Save the bloom
    bloom.save()

    # Capture the mtime on the save files
    bloom_file = testing_dir.join('bloom.npy')
    meta_file = testing_dir.join('meta.json')
    first_bloom_save = bloom_file.mtime()
    first_meta_save = meta_file.mtime()

    # Sleep for 1 second to deal ensure mtimes will advance
    time.sleep(1)

    # Reload the bloom
    second_gen_bloom = TimingBloomFilter.load(temp_path)

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
    third_gen_bloom = TimingBloomFilter.load(temp_path)

    # Check that the loaded data is as expected
    assert third_gen_bloom.contains('50')
    assert third_gen_bloom.contains('103')
    assert not third_gen_bloom.contains('105')
