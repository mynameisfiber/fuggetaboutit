from copy import copy

from mock import MagicMock, patch, sentinel
import numpy as np
import pytest

from fuggetaboutit.timing_bloom_filter import TimingBloomFilter

from ..utils import assert_bloom_values

BLOOM_DEFAULTS = {
    'capacity': 1000,
    'error': 0.0002,
    'data_path': '/some/path/',
    'id': 1,
    'decay_time': 86400, # 24 hours
    'disable_optimizations': False,
}


def get_bloom(**overrides):
    '''
    Helper function to easily get a bloom for testing.
    '''
    kwargs = copy(BLOOM_DEFAULTS)
    kwargs.update(overrides)

    return TimingBloomFilter(**kwargs)


def assert_empty_bloom(bloom):
    '''
    Helper function to test that a bloom has no data.
    '''
    assert 0 == np.count_nonzero(bloom.data)
    assert bloom.num_non_zero == 0


def test_init_no_bloom_data():
    # Setup test data
    capacity = 1000
    error = 0.002
    decay_time = 86400
    data_path = '/does/not/exist'
    id = 5

    # Call init and get back a bloom
    bloom = TimingBloomFilter(
        capacity=capacity,
        error=error,
        decay_time=decay_time,
        data_path=data_path,
        id=id,
    )

    # Make sure the bloom is setup as expected
    assert_bloom_values(bloom, {
        'capacity': capacity,
        'error': error,
        'data_path': data_path,
        'id': id,
        'num_bytes': 12935,
        'num_hashes': 9,
        'ring_size': 15,
        'dN': 7,
        'seconds_per_tick': 12342.857142857143,
        '_optimize': True,
    })

    test_dp, test_mf, test_bf = bloom._get_paths(None)
    assert test_dp == '/does/not/exist'
    assert test_mf == '/does/not/exist/meta.json'
    assert test_bf == '/does/not/exist/bloom.npy'

    assert_empty_bloom(bloom)


@patch('numpy.load')
@patch('os.path.exists')
def test_init_with_bloom_data(exists_mock, load_mock):
    # Setup test data and mocks
    exists_mock.return_value = True
    load_mock.return_value = sentinel.data

    capacity = 1000
    error = 0.002
    decay_time = 86400
    data_path = '/does/not/exist'

    # Call init
    bloom = TimingBloomFilter(
        capacity=capacity,
        error=error,
        decay_time=decay_time,
        data_path=data_path,
    )

    # Check that the bloom is setup as expected
    assert_bloom_values(bloom, {
        'capacity': capacity,
        'error': error,
        'data_path': data_path,
        'id': None,
        'num_bytes': 12935,
        'num_hashes': 9,
        'ring_size': 15,
        'dN': 7,
        'seconds_per_tick': 12342.857142857143,
        '_optimize': True,
        'data': sentinel.data,
    })

    test_dp, test_mf, test_bf = bloom._get_paths(None)
    assert test_dp == '/does/not/exist'
    assert test_mf == '/does/not/exist/meta.json'
    assert test_bf == '/does/not/exist/bloom.npy'

    exists_mock.assert_called_once_with(test_bf)
    load_mock.assert_called_once_with(test_bf)


@patch('time.time')
def test_get_tick_current_ts(time_mock):
    # Get a bloom
    bloom = get_bloom()

    # Setup mocks
    time_mock.return_value = 1388056353.436583

    # Call tick
    tick_val = bloom.get_tick()

    # Check results
    expected_tick_val = 4
    assert expected_tick_val == tick_val
    time_mock.assert_called_once_with()


@patch('time.time')
def test_get_tick_given_ts(time_mock):
    # Get a bloom
    bloom = get_bloom()

    # Setup test data
    target_ts = 1388092483

    # Call tick
    tick_val = bloom.get_tick(target_ts)

    # Check results
    expected_tick_val = 7
    assert expected_tick_val == tick_val
    assert 0 == time_mock.call_count


@patch('time.time')
def test_get_tick_range(time_mock):
    # Get a bloom
    bloom = get_bloom()

    # Setup mocks
    time_mock.return_value = 1388056353.436583

    # Call tick range
    tick_range = bloom.get_tick_range()

    # Check results
    expected_range = 12, 4
    assert expected_range == tick_range


def test_get_interval_test_min_lt_max():
    # Get a bloom
    bloom = get_bloom()

    # Setup mock
    range_mock = MagicMock(bloom.get_tick_range)
    range_mock.return_value = 4, 12
    bloom.get_tick_range = range_mock

    # Get interval test
    test = bloom.get_interval_test()

    # Check that the test works as expected
    assert not test(3)
    assert not test(4)
    assert test(5)
    assert test(12)
    assert not test(13)


def test_get_interval_test_min_gt_max():
    # Get a bloom
    bloom = get_bloom()

    # Setup mock
    range_mock = MagicMock(bloom.get_tick_range)
    range_mock.return_value = 12, 4
    bloom.get_tick_range = range_mock

    # Get interval test
    test = bloom.get_interval_test()

    # Check that the test works as expected
    assert test(3)
    assert test(4)
    assert not test(5)
    assert not test(12)
    assert test(13)


def test_add_expired():
    # Get a bloom
    bloom = get_bloom()

    # Get a timestamp from way in the past
    timestamp = 652147200 # 9/1/1990

    # Add an entry with the timestamp
    key = 'test'
    bloom.add(key, timestamp)

    # Check that the bloom is still empty
    assert_empty_bloom(bloom)


@patch('time.time')
def test_add_with_optimizations_and_current_time(time_mock):
    # Get a bloom
    bloom = get_bloom()

    # Setup mocks and test data
    time_mock.return_value = 1388159391.882157
    key = 'test'

    # Add the key
    bloom.add(key)

    # Check that the bloom is in the expected state
    expected_num_non_zero = 12
    assert expected_num_non_zero == bloom.num_non_zero
    assert expected_num_non_zero == np.count_nonzero(bloom.data)


@patch('time.time')
def test_add_with_optimizations_and_timestamp(time_mock):
    # Get a bloom
    bloom = get_bloom()

    # Setup mocks and test data
    time_mock.return_value = 1388159391.882157
    timestamp = time_mock.return_value - 12343  # Makes timestamp fall in previous tick
    key = 'test'

    # Add the key
    bloom.add(key, timestamp)

    # Check that the bloom is in the expected state
    expected_num_non_zero = 12
    assert expected_num_non_zero == bloom.num_non_zero
    assert expected_num_non_zero == np.count_nonzero(bloom.data)


@patch('time.time')
def test_add_without_optimizations_and_current_time(time_mock):
    # Get a bloom
    bloom = get_bloom(disable_optimizations=True)
    assert not bloom._optimize

    # Setup mocks and test data
    time_mock.return_value = 1388159391.882157
    key = 'test'

    # Add the key
    bloom.add(key)

    # Check that the bloom is in the expected state
    expected_num_non_zero = 12
    assert expected_num_non_zero == bloom.num_non_zero
    assert expected_num_non_zero == np.count_nonzero(bloom.data)


@patch('time.time')
def test_add_without_optimizations_and_timestamp(time_mock):
    # Get a bloom
    bloom = get_bloom(disable_optimizations=True)
    assert not bloom._optimize

    # Setup mocks and test data
    time_mock.return_value = 1388159391.882157
    timestamp = time_mock.return_value - 12343  # Makes timestamp fall in previous tick
    key = 'test'

    # Add the key
    bloom.add(key, timestamp)

    # Check that the bloom is in the expected state
    expected_num_non_zero = 12
    assert expected_num_non_zero == bloom.num_non_zero
    assert expected_num_non_zero == np.count_nonzero(bloom.data)


@patch('time.time')
def test_contains_with_optimization(time_mock):
    # Get a bloom
    bloom = get_bloom()

    # Setup mocks and test data
    time_mock.return_value = 1388159391.882157
    valid_keys = ['test', 'foo', 'fizz']
    invalid_keys = ['bar', 'buzz']

    # Add a few keys
    for key in valid_keys:
        bloom.add(key)

    # Check that contains returns the expected results
    for key in valid_keys:
        assert bloom.contains(key)

    for key in invalid_keys:
        assert not bloom.contains(key)


@patch('time.time')
def test_contains_without_optimization(time_mock):
    # Get a bloom
    bloom = get_bloom(disable_optimizations=True)
    assert not bloom._optimize

    # Setup mocks and test data
    time_mock.return_value = 1388159391.882157
    valid_keys = ['test', 'foo', 'fizz']
    invalid_keys = ['bar', 'buzz']

    # Add a few keys
    for key in valid_keys:
        bloom.add(key)

    # Check that contains returns the expected results
    for key in valid_keys:
        assert bloom.contains(key)

    for key in invalid_keys:
        assert not bloom.contains(key)


@patch('time.time')
def test_decay_with_optimizations(time_mock):
    # Get a bloom
    bloom = get_bloom()

    # Setup mocks and test data
    time_mock.return_value = 1388159391.882157
    key = 'test'

    # Add the key
    bloom.add(key)

    # Check that the bloom is in the expected state
    expected_num_non_zero = 12
    assert expected_num_non_zero == bloom.num_non_zero
    assert expected_num_non_zero == np.count_nonzero(bloom.data)

    # Perform initial decay which should be a no-op
    bloom.decay()
    
    # Check that the bloom is in the expected state
    expected_num_non_zero = 12
    assert expected_num_non_zero == bloom.num_non_zero
    assert expected_num_non_zero == np.count_nonzero(bloom.data)

    # Move time forward and decay until bloom is empty
    for n in range(1, 8):
        time_mock.return_value -= 12343
        bloom.decay()

    assert_empty_bloom(bloom)


@patch('time.time')
def test_decay_without_optimizations(time_mock):
    # Get a bloom
    bloom = get_bloom(disable_optimizations=True)
    assert not bloom._optimize

    # Setup mocks and test data
    time_mock.return_value = 1388159391.882157
    key = 'test'

    # Add the key
    bloom.add(key)

    # Check that the bloom is in the expected state
    expected_num_non_zero = 12
    assert expected_num_non_zero == bloom.num_non_zero
    assert expected_num_non_zero == np.count_nonzero(bloom.data)

    # Perform initial decay which should be a no-op
    bloom.decay()
    
    # Check that the bloom is in the expected state
    expected_num_non_zero = 12
    assert expected_num_non_zero == bloom.num_non_zero
    assert expected_num_non_zero == np.count_nonzero(bloom.data)

    # Move time forward and decay until bloom is empty
    for n in range(1, 8):
        time_mock.return_value -= 12343
        bloom.decay()

    assert_empty_bloom(bloom)


def test_get_meta():
    # Get a bloom
    bloom = get_bloom()

    # Make sure the meta data gets returned as expected
    expected_meta = copy(BLOOM_DEFAULTS)
    del expected_meta['data_path']
    assert expected_meta == bloom.get_meta()


def test_remove():
    # Get a bloom
    bloom = get_bloom()

    # Call remove
    with pytest.raises(NotImplementedError):
        bloom.remove('test')


def test_remove_all():
    # Get a bloom
    bloom = get_bloom()

    # Call remove_all
    with pytest.raises(NotImplementedError):
        bloom.remove_all('test')
