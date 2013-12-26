from copy import copy

from mock import MagicMock, patch, sentinel
import numpy as np

from fuggetaboutit.timing_bloom_filter import TimingBloomFilter

BLOOM_DEFAULTS = {
    'capacity': 1000,
    'error': 0.0002,
    'data_path': '/some/path/',
    'id': 1,
    'decay_time': 86400, # 24 hours
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
    assert capacity == bloom.capacity
    assert error == bloom.error
    assert data_path == bloom.data_path
    assert decay_time == bloom.decay_time
    assert id == bloom.id

    expected_num_bytes = 12935
    assert expected_num_bytes == bloom.num_bytes
    expected_num_hashes = 9
    assert expected_num_hashes == bloom.num_hashes
    expected_ring_size = 15
    assert expected_ring_size == bloom.ring_size
    expected_dN = 7
    assert expected_dN == bloom.dN
    expected_seconds_per_tick = 12342.857142857143
    assert expected_seconds_per_tick == bloom.seconds_per_tick
    assert bloom._optimize, "Optimizations are not applied, make sure C extension is installed properly"

    expected_bloom_filename = '/does/not/exist/bloom.npy'
    assert expected_bloom_filename == bloom.bloom_filename
    expected_meta_filename = '/does/not/exist/meta.json'
    assert expected_meta_filename == bloom.meta_filename

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
    assert capacity == bloom.capacity
    assert error == bloom.error
    assert data_path == bloom.data_path
    assert decay_time == bloom.decay_time
    assert None == bloom.id

    expected_num_bytes = 12935
    assert expected_num_bytes == bloom.num_bytes
    expected_num_hashes = 9
    assert expected_num_hashes == bloom.num_hashes
    expected_ring_size = 15
    assert expected_ring_size == bloom.ring_size
    expected_dN = 7
    assert expected_dN == bloom.dN
    expected_seconds_per_tick = 12342.857142857143
    assert expected_seconds_per_tick == bloom.seconds_per_tick
    assert bloom._optimize, "Optimizations are not applied, make sure C extension is installed properly"

    expected_bloom_filename = '/does/not/exist/bloom.npy'
    assert expected_bloom_filename == bloom.bloom_filename
    expected_meta_filename = '/does/not/exist/meta.json'
    assert expected_meta_filename == bloom.meta_filename

    exists_mock.assert_called_once_with(bloom.bloom_filename)
    load_mock.assert_called_once_with(bloom.bloom_filename)
    expected_bloom_data = sentinel.data
    assert expected_bloom_data == bloom.data


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