from copy import copy
import json

from mock import MagicMock, mock_open, patch
import pytest

from fuggetaboutit.exceptions import PersistenceDisabledException
from fuggetaboutit.scaling_timing_bloom_filter import ScalingTimingBloomFilter
from fuggetaboutit.timing_bloom_filter import TimingBloomFilter
from fuggetaboutit.tickers import NoOpTicker
from fuggetaboutit.scaling_timing_bloom_filter import _get_paths

from ..utils import assert_bloom_values


BLOOM_DEFAULTS = {
    'capacity': 1000,
    'error': 0.0002,
    'data_path': '/some/path/',
    'decay_time': 86400, # 24 hours
    'disable_optimizations': False,
}

TIMING_BLOOM_DEFAULTS = {
    'seconds_per_tick': 100
}


def get_bloom(bloom_mocks=None, **overrides):
    '''
    Helper function to easily get a bloom for testing.
    '''
    kwargs = copy(BLOOM_DEFAULTS)
    kwargs.update(overrides)

    if bloom_mocks:
        blooms = []
        for mock_info in bloom_mocks:
            mock = MagicMock(TimingBloomFilter)
            mock_attrs = copy(TIMING_BLOOM_DEFAULTS)
            mock_attrs.update(mock_info.get('attrs', {}))
            for key, value in mock_attrs.iteritems():
                setattr(mock, key, value)

            for key, value in mock_info.get('return_values', {}).iteritems():
                attr = getattr(mock, key)
                attr.return_value = value

            blooms.append(mock)
        kwargs['blooms'] = blooms


    return ScalingTimingBloomFilter(**kwargs)


@patch('fuggetaboutit.scaling_timing_bloom_filter.TimingBloomFilter')
def test_basic_init(timing_bloom_mock):
    # Setup test data
    error = 0.0002
    capacity = 1000
    decay_time = 86400

    # Create bloom
    bloom = ScalingTimingBloomFilter(error=error, capacity=capacity, decay_time=decay_time)

    assert_bloom_values(bloom, {
        'error': error,
        'capacity': capacity,
        'decay_time': decay_time,
        'error_tightening_ratio': 0.5,
        'error_initial': 0.0001,
        'growth_factor': 2,
        'max_fill_factor': 0.8,
        'min_fill_factor': 0.2,
        'insert_tail': True,
        'data_path': None,
        'seconds_per_tick': bloom.blooms[0].seconds_per_tick,
        'disable_optimizations': False,
    })

    expected_blooms_len = 1
    assert expected_blooms_len == len(bloom.blooms)
    
    timing_bloom_mock.assert_called_once_with(
        capacity=693,
        decay_time=decay_time,
        error=0.0001,
        id=0,
        disable_optimizations=False
    )

    expected_ticker_class = NoOpTicker
    assert isinstance(bloom.ticker, expected_ticker_class)


@patch('fuggetaboutit.scaling_timing_bloom_filter.TimingBloomFilter')
def test_init_without_optimizations(timing_bloom_mock):
    # Setup test data
    error = 0.0002
    capacity = 1000
    decay_time = 86400
    disable_optimizations = True

    # Create bloom
    bloom = ScalingTimingBloomFilter(
        error=error,
        capacity=capacity,
        decay_time=decay_time,
        disable_optimizations=disable_optimizations,
    )

    assert_bloom_values(bloom, {
        'error': error,
        'capacity': capacity,
        'decay_time': decay_time,
        'error_tightening_ratio': 0.5,
        'error_initial': 0.0001,
        'growth_factor': 2,
        'max_fill_factor': 0.8,
        'min_fill_factor': 0.2,
        'insert_tail': True,
        'data_path': None,
        'seconds_per_tick': bloom.blooms[0].seconds_per_tick,
        'disable_optimizations': disable_optimizations,
    })

    expected_blooms_len = 1
    assert expected_blooms_len == len(bloom.blooms)
    
    timing_bloom_mock.assert_called_once_with(
        capacity=693,
        decay_time=decay_time,
        error=0.0001,
        id=0,
        disable_optimizations=disable_optimizations,
    )

    expected_ticker_class = NoOpTicker
    assert isinstance(bloom.ticker, expected_ticker_class)


@patch('fuggetaboutit.scaling_timing_bloom_filter.TimingBloomFilter')
def test_init_without_growth_factor(timing_bloom_mock):
    # Setup test data
    error = 0.0002
    capacity = 1000
    decay_time = 86400
    growth_factor = None

    # Create bloom
    bloom = ScalingTimingBloomFilter(
        error=error,
        capacity=capacity,
        decay_time=decay_time,
        growth_factor=growth_factor,
    )

    assert_bloom_values(bloom, {
        'error': error,
        'capacity': capacity,
        'decay_time': decay_time,
        'error_tightening_ratio': 0.5,
        'error_initial': 0.0001,
        'growth_factor': growth_factor,
        'max_fill_factor': 0.8,
        'min_fill_factor': 0.2,
        'insert_tail': True,
        'data_path': None,
        'seconds_per_tick': bloom.blooms[0].seconds_per_tick,
        'disable_optimizations': False,
    })

    expected_blooms_len = 1
    assert expected_blooms_len == len(bloom.blooms)
    
    timing_bloom_mock.assert_called_once_with(
        capacity=capacity,
        decay_time=decay_time,
        error=0.0001,
        id=0,
        disable_optimizations=False,
    )


    expected_ticker_class = NoOpTicker
    assert isinstance(bloom.ticker, expected_ticker_class)


def test_full_init():
    # Setup test data
    error = 0.0002
    capacity = 1000
    decay_time = 86400
    ticker = MagicMock(NoOpTicker)
    data_path = '/foo/bar/baz'
    error_tightening_ratio = 0.4
    growth_factor = 3
    min_fill_factor = 0.1
    max_fill_factor = 0.9
    insert_tail = False
    disable_optimizations = True

    timing_bloom = MagicMock(TimingBloomFilter)
    timing_bloom.seconds_per_tick = 100
    blooms = [timing_bloom]

    # Create the bloom
    bloom = ScalingTimingBloomFilter(
        error=error,
        capacity=capacity,
        decay_time=decay_time,
        ticker=ticker,
        data_path=data_path,
        error_tightening_ratio=error_tightening_ratio,
        growth_factor=growth_factor,
        min_fill_factor=min_fill_factor,
        max_fill_factor=max_fill_factor,
        insert_tail=insert_tail,
        blooms=blooms,
        disable_optimizations=disable_optimizations,
    )

    # Check that the bloom's state matches expectations
    assert_bloom_values(bloom, {
        'error': error,
        'capacity': capacity,
        'decay_time': decay_time,
        'error_tightening_ratio': error_tightening_ratio,
        'growth_factor': growth_factor,
        'max_fill_factor': max_fill_factor,
        'min_fill_factor': min_fill_factor,
        'insert_tail': insert_tail,
        'data_path': data_path,
        'blooms': blooms,
        'error_initial': 0.00012,
        'seconds_per_tick': timing_bloom.seconds_per_tick,
        'disable_optimizations': disable_optimizations,
    })

    data_path, meta_filename, blooms_path = _get_paths(bloom.data_path, None)
    assert meta_filename == '/foo/bar/baz/meta.json'
    assert blooms_path == '/foo/bar/baz/blooms'

    ticker.setup.assert_called_once_with(bloom.decay, timing_bloom.seconds_per_tick)
    ticker.start.assert_called_once_with()


@patch('fuggetaboutit.scaling_timing_bloom_filter.TimingBloomFilter')
def test_get_next_id_empty(timing_bloom_mock):
    # Get a bloom
    bloom = get_bloom()

    # Set blooms to an empty list []
    bloom.blooms = []

    # Call get next id
    next_id = bloom._get_next_id()

    # Check the result
    expected_next_id = 0
    assert expected_next_id == next_id


@patch('fuggetaboutit.scaling_timing_bloom_filter.TimingBloomFilter')
def test_get_next_id_existing(timing_bloom_mock):
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[{'attrs': {'id': 1}}, {'attrs': {'id': 100}}])

    # Call get next id 
    next_id = bloom._get_next_id()

    # Check the result
    expected_next_id = 101
    assert expected_next_id == next_id


@patch('fuggetaboutit.scaling_timing_bloom_filter.TimingBloomFilter')
def test_get_bloom_path(timing_bloom_mock):
    bloom = get_bloom(data_path='/test/path')
    _, _, blooms_path = _get_paths(bloom.data_path, None)
    path = bloom.get_bloom_path(blooms_path, bloom_id=5)
    assert path == '/test/path/blooms/5'


def test_get_size():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'return_values': {'get_size': 5}},
        {'return_values': {'get_size': 10}},
    ])

    # Call get_size
    size = bloom.get_size()

    # Check results
    expected_size = 15
    assert expected_size == size


@patch('fuggetaboutit.scaling_timing_bloom_filter.TimingBloomFilter')
def test_expected_error_empty(timing_bloom_mock):
    # Get a bloom
    bloom = get_bloom()

    # Clear out the timing blooms
    bloom.blooms = []

    # Call get_expected_error
    error = bloom.get_expected_error()

    # Check results
    expected_error = 0.0
    assert expected_error == error


def test_expected_error():
    # Get a bloom
    bloom = get_bloom([
        {'attrs': {'error': 0.002}},
        {'attrs': {'error': 0.005}},
    ])

    # Call get_expected_error
    error = bloom.get_expected_error()

    # Check results
    expected_error = 0.006990000000000052
    assert expected_error == error


def test_get_capacity_for_id_with_growth():
    # Get a bloom
    bloom = get_bloom(growth_factor=3, bloom_mocks=[{}])

    # Call get capacity for id 2
    capacity = bloom.get_capacity_for_id(2)

    # Check results
    expected_capacity = 6238
    assert expected_capacity == capacity

    # Call get capacity for id 3
    capacity = bloom.get_capacity_for_id(3)

    # Check results
    expected_capacity = 18714
    assert expected_capacity == capacity


def test_get_capacity_for_id_without_growth():
    # Get a bloom
    bloom = get_bloom(growth_factor=None, bloom_mocks=[{}])

    # Call get capacity for id 2
    capacity = bloom.get_capacity_for_id(2)

    # Check results
    expected_capacity = 1000
    assert expected_capacity == capacity

    # Call get capacity for id 3
    capacity = bloom.get_capacity_for_id(3)

    # Check results
    expected_capacity = 1000
    assert expected_capacity == capacity


@patch('fuggetaboutit.scaling_timing_bloom_filter.TimingBloomFilter')
def test_add_new_bloom_without_id(timing_bloom_mock):
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[{'attrs': {'id': 0}}])
    original_bloom = bloom.blooms[0]

    # Setup mocks
    bloom.get_capacity_for_id = MagicMock(bloom.get_capacity_for_id)
    bloom.get_capacity_for_id.return_value = 1234

    # Call add bloom
    returned_bloom = bloom._add_new_bloom()

    # Check that the bloom was created with the expected state
    timing_bloom_mock.assert_called_once_with(
        capacity=1234,
        decay_time=86400,
        error=0.00005,
        id=1,
        disable_optimizations=False,
    )

    # Check that the returned bloom is the correct bloom
    assert timing_bloom_mock.return_value == returned_bloom

    # Check that the blooms array has the expected state
    expected_blooms = [original_bloom, returned_bloom]
    assert expected_blooms == bloom.blooms

    # Check that other methods were called as expected
    bloom.get_capacity_for_id.assert_called_once_with(1)


@patch('fuggetaboutit.scaling_timing_bloom_filter.TimingBloomFilter')
def test_add_new_bloom_with_id(timing_bloom_mock):
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[{'attrs': {'id': 0}}])
    original_bloom = bloom.blooms[0]

    # Setup mocks
    bloom.get_capacity_for_id = MagicMock(bloom.get_capacity_for_id)
    bloom.get_capacity_for_id.return_value = 1234

    # Call add bloom
    returned_bloom = bloom._add_new_bloom(bloom_id=5)

    # Check that the bloom was created with the expected state
    timing_bloom_mock.assert_called_once_with(
        capacity=1234,
        decay_time=86400,
        error=3.125e-06,
        id=5,
        disable_optimizations=False,
    )

    # Check that the returned bloom is the correct bloom
    assert timing_bloom_mock.return_value == returned_bloom

    # Check that the blooms array has the expected state
    expected_blooms = [original_bloom, returned_bloom]
    assert expected_blooms == bloom.blooms

    # Check that other methods were called as expected
    bloom.get_capacity_for_id.assert_called_once_with(5)


def test_get_bloom_iter_insert_tail():
    # Get a bloom
    bloom = get_bloom(insert_tail=True, bloom_mocks=[
        {'attrs': {'id': 1}},
        {'attrs': {'id': 2}},
    ])

    # Call get bloom iter
    bloom_iter = bloom.get_bloom_iter()

    # Check results
    expected_ids = [2, 1]
    assert expected_ids == [subbloom.id for subbloom in bloom_iter]


def test_get_bloom_iter_not_insert_tail():
    # Get a bloom
    bloom = get_bloom(insert_tail=False, bloom_mocks=[
        {'attrs': {'id': 1}},
        {'attrs': {'id': 2}},
    ])

    # Call get bloom iter
    bloom_iter = bloom.get_bloom_iter()

    # Check results
    expected_ids = [1, 2]
    assert expected_ids == [subbloom.id for subbloom in bloom_iter]


def test_get_active_bloom_simple():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'return_values': {'get_size': 4.000002342}, 'attrs': {'capacity': 1000}},
    ])
    bloom._add_new_bloom = MagicMock(bloom._add_new_bloom)

    # Call get active bloom
    active_bloom = bloom.get_active_bloom()

    # Check that the returned bloom is the expected bloom
    expected_bloom = bloom.blooms[0]
    assert expected_bloom == active_bloom

    # Check that the bloom hasn't scaled
    expected_num_blooms = 1
    assert expected_num_blooms == len(bloom.blooms)
    assert not bloom._add_new_bloom.called

    # Check that the bloom was inspected as expected
    expected_bloom.get_size.assert_called_once_with()


def test_get_active_bloom_skip():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'return_values': {'get_size': 4.000002342}, 'attrs': {'capacity': 1000}},
        {'return_values': {'get_size': 1999.000002342}, 'attrs': {'capacity': 2000}},
    ])
    bloom._add_new_bloom = MagicMock(bloom._add_new_bloom)

    # Call get active bloom
    active_bloom = bloom.get_active_bloom()

    # Check that the returned bloom is the expected bloom
    expected_bloom = bloom.blooms[0]
    assert expected_bloom == active_bloom

    # Check that the bloom hasn't scaled
    expected_num_blooms = 2
    assert expected_num_blooms == len(bloom.blooms)
    assert not bloom._add_new_bloom.called

    # Check that the blooms were inspected as expected
    for sub_bloom in bloom.blooms:
        sub_bloom.get_size.assert_called_once_with()


def test_get_active_bloom_scale():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'return_values': {'get_size': 1999.000002342}, 'attrs': {'capacity': 2000}},
    ])

    # Setup mocks
    bloom._add_new_bloom = MagicMock(bloom._add_new_bloom)
    new_bloom = MagicMock(TimingBloomFilter)
    bloom._add_new_bloom.return_value = new_bloom

    # Call get active bloom
    active_bloom = bloom.get_active_bloom()

    # Check that the returned bloom is the expected bloom
    expected_bloom = new_bloom
    assert expected_bloom == active_bloom

    # Check that the bloom has scaled
    bloom._add_new_bloom.assert_called_once_with()

    # Check that the bloom was inspected as expected
    bloom.blooms[0].get_size.assert_called_once_with()


def test_add_simple():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[{}])

    # Setup mocks
    bloom.get_active_bloom = MagicMock(bloom._add_new_bloom)
    sub_bloom_mock = bloom.blooms[0]
    bloom.get_active_bloom.return_value = sub_bloom_mock

    # Do the add
    key = 'test'
    bloom.add(key)

    # Check that the mock has the expected state
    sub_bloom_mock.add.assert_called_once_with(key, None)


def test_add_with_timestamp():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[{}])

    # Setup mocks
    bloom.get_active_bloom = MagicMock(bloom._add_new_bloom)
    sub_bloom_mock = bloom.blooms[0]
    bloom.get_active_bloom.return_value = sub_bloom_mock

    # Do the add
    key = 'test'
    timestamp = 1388056353.436583
    bloom.add(key, timestamp)

    # Check that the mock has the expected state
    sub_bloom_mock.add.assert_called_once_with(key, timestamp)


def test_contains_hit():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'return_values': {'contains': False}},
        {'return_values': {'contains': True}},
    ])

    # Call contains
    key = 'test'
    result = bloom.contains(key)

    # Check the result
    expected_result = True
    assert expected_result == result

    # Check that the sub-blooms were checked as expected
    for sub_bloom in bloom.blooms:
        sub_bloom.contains.assert_called_once_with(key)


def test_contains_miss():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'return_values': {'contains': False}},
        {'return_values': {'contains': False}},
    ])

    # Call contains
    key = 'test'
    result = bloom.contains(key)

    # Check the result
    expected_result = False
    assert expected_result == result

    # Check that the sub-blooms were checked as expected
    for sub_bloom in bloom.blooms:
        sub_bloom.contains.assert_called_once_with(key)


@patch('shutil.rmtree')
def test_cleanup_empty_blooms_noop(rmtree_mock):
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'attrs': {'num_non_zero': 123, 'capacity': 2000, 'error': 0.00005}},
        {'attrs': {'num_non_zero': 5, 'capacity': 1000, 'error': 0.00005}},
    ])

    # Call cleanup
    cleaned = bloom.cleanup_empty_blooms()

    # Check results
    expected_cleaned = 0
    assert expected_cleaned == cleaned

    # Check that rmtree wasn't called
    assert not rmtree_mock.called

    # Check that the blooms array hasn't been trimmed
    expected_len = 2
    assert expected_len == len(bloom.blooms)


@patch('fuggetaboutit.scaling_timing_bloom_filter.rmtree')
def test_cleanup_empty_blooms_trim(rmtree_mock):
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'attrs': {'id': 1, 'num_non_zero': 123, 'capacity': 2000, 'error': 0.00005}},
        {'attrs': {'id': 2, 'num_non_zero': 0, 'capacity': 1000, 'error': 0.00005, 'data_path': '/does/not/exist'}},
    ])
    keep_bloom = bloom.blooms[0]
    kill_bloom = bloom.blooms[1]

    # Call cleanup
    cleaned = bloom.cleanup_empty_blooms()

    # Check results
    expected_cleaned = 1
    assert expected_cleaned == cleaned

    # Check that rmtree was called as expected
    rmtree_mock.assert_called_once_with(kill_bloom.data_path)

    # Check that the blooms array has been trimmed
    expected_bloom_ids = [keep_bloom.id]
    assert expected_bloom_ids == [b.id for b in bloom.blooms]


def test_try_to_shrink__noop__id_0():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'attrs': {'id': 0, 'capacity': 2000}, 'return_values': {'get_size': 2}},
    ])
    bloom._add_new_bloom = MagicMock(bloom._add_new_bloom)

    # Try to shrink
    did_shrink = bloom.try_to_shrink()

    # Check result
    assert not did_shrink

    # Check that the bloom got inspected as expected
    bloom.blooms[0].get_size.assert_called_once_with()

    # Check that add new bloom was not called
    assert not bloom._add_new_bloom.called


def test_try_to_shrink__noop__over_capacity():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'attrs': {'id': 5, 'capacity': 2000}, 'return_values': {'get_size': 1000}},
    ])
    bloom._add_new_bloom = MagicMock(bloom._add_new_bloom)

    # Try to shrink
    did_shrink = bloom.try_to_shrink()

    # Check result
    assert not did_shrink

    # Check that the bloom got inspected as expected
    bloom.blooms[0].get_size.assert_called_once_with()

    # Check that add new bloom was not called
    assert not bloom._add_new_bloom.called


def test_try_to_shrink__noop__too_many_blooms():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'attrs': {'id': 4, 'capacity': 2000}, 'return_values': {'get_size': 1000}},
        {'attrs': {'id': 5, 'capacity': 2000}, 'return_values': {'get_size': 1000}},
    ])
    bloom._add_new_bloom = MagicMock(bloom._add_new_bloom)

    # Try to shrink
    did_shrink = bloom.try_to_shrink()

    # Check result
    assert not did_shrink

    # Check that the blooms did not get inspected as expected
    assert not any(b.get_size.called for b in bloom.blooms)

    # Check that add new bloom was not called
    assert not bloom._add_new_bloom.called


def test_try_to_shrink__shrink():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[
        {'attrs': {'id': 5, 'capacity': 2000}, 'return_values': {'get_size': 1}},
    ])
    bloom._add_new_bloom = MagicMock(bloom._add_new_bloom)

    # Try to shrink
    did_shrink = bloom.try_to_shrink()

    # Check result
    assert did_shrink

    # Check that the bloom got inspected as expected
    bloom.blooms[0].get_size.assert_called_once_with()

    # Check that add new bloom was called
    bloom._add_new_bloom.assert_called_once_with(4)


def test_decay():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[{}])

    # Setup mocks
    bloom.cleanup_empty_blooms = MagicMock(bloom.cleanup_empty_blooms)
    bloom.try_to_shrink = MagicMock(bloom.try_to_shrink)

    # Call decay
    bloom.decay()

    # Check that decay on the sub-blooms got called
    for sub in bloom.blooms:
        sub.decay.assert_called_once_with()

    # Check that the follow-up methods were called
    bloom.cleanup_empty_blooms.assert_called_once_with()
    bloom.try_to_shrink.assert_called_once_with()


def test_start():
    # Get a bloom and a ticker
    ticker_mock = MagicMock(NoOpTicker)
    bloom = get_bloom(bloom_mocks=[{}])
    bloom.ticker = ticker_mock

    # Call start
    bloom.start()

    # Check that start() on the ticker got called as expected
    ticker_mock.start.assert_called_once_with()


def test_stop():
    # Get a bloom and a ticker
    ticker_mock = MagicMock(NoOpTicker)
    bloom = get_bloom(bloom_mocks=[{}])
    bloom.ticker = ticker_mock

    # Call start
    bloom.stop()

    # Check that start() on the ticker got called as expected
    ticker_mock.stop.assert_called_once_with()


def test_get_meta():
    # Get a bloom
    config = {
        'capacity': 500,
        'decay_time': 30,
        'error': 0.5,
        'error_tightening_ratio': 0.2,
        'growth_factor': 5,
        'min_fill_factor': 0.1,
        'max_fill_factor': 0.9,
        'insert_tail': False,
        'disable_optimizations': True,
    }
    bloom = get_bloom(bloom_mocks=[{}], **config)

    # Call get meta
    meta = bloom.get_meta()

    # Check the results
    assert config == meta


def test_check_data_path__no_data_path():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[{}], data_path=None)

    # Call check data path and make sure it raises the expected exception
    with pytest.raises(PersistenceDisabledException):
        _get_paths(bloom.data_path, None)


@patch('os.path.isdir')
@patch('os.listdir')
def test_discover_blooms(listdir_mock, isdir_mock):
    # Setup mocks
    listdir_mock.return_value = ['/path/1', '/path/2', '/path/3']
    isdir_mock.side_effect = [True, False, True]

    # Call discover blooms
    paths = ScalingTimingBloomFilter.discover_blooms('/path')

    # Check results
    expected_paths = ['/path/1', '/path/3']
    assert expected_paths == paths

    # Check that system calls were made as expected
    listdir_mock.assert_called_once_with('/path')

    for path in listdir_mock.return_value:
        isdir_mock.assert_any_call(path)


@patch('fuggetaboutit.scaling_timing_bloom_filter.TimingBloomFilter')
def test_load__with_blooms(timing_bloom_mock):
    # Setup test data 
    test_data = {
        'capacity': 500,
        'decay_time': 30,
        'error': 0.5,
        'error_tightening_ratio': 0.2,
        'growth_factor': 5,
        'min_fill_factor': 0.1,
        'max_fill_factor': 0.9,
        'insert_tail': False,
        'disable_optimizations': True,
    }
    open_mock = mock_open(read_data=json.dumps(test_data))
    data_path = '/test/foo/bar'
    bloom_paths = ['/test/foo/bar/blooms/1', '/test/foo/bar/blooms/2']
    ScalingTimingBloomFilter.discover_blooms = MagicMock(ScalingTimingBloomFilter)
    ScalingTimingBloomFilter.discover_blooms.return_value = bloom_paths

    # Call load
    with patch('__builtin__.open', open_mock, create=True):
        loaded = ScalingTimingBloomFilter.load(data_path)


    # Check that metadata was opened as expected
    open_mock.assert_called_once_with(data_path + '/meta.json', 'r')

    # Check that the loaded bloom looks as expected
    for key, value in test_data.iteritems():
        assert value == getattr(loaded, key)
    expected_bloom_count = 2
    assert expected_bloom_count == len(loaded.blooms)

    # Check that the sub blooms were loaded as expected
    for path in bloom_paths:
        timing_bloom_mock.load.assert_any_call(path)
    expected_load_calls = len(bloom_paths)
    assert expected_load_calls == timing_bloom_mock.load.call_count


def test_contains_magic():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[{}])
    bloom.contains = MagicMock(bloom.contains)
    bloom.contains.return_value = True

    # Call __contains
    key = 'test'
    result = key in bloom

    # Check results and calls
    assert result
    bloom.contains.assert_called_once_with(key)


def test_add_magic():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[{}])
    bloom.add = MagicMock(bloom.add)
    bloom.add.return_value = None

    # Perform the add operation
    key = 'test'
    bloom += key

    # Check that the expected calls were made
    bloom.add.assert_called_once_with(key)


def test_len_magic():
    # Get a bloom
    bloom = get_bloom(bloom_mocks=[{}])
    bloom.get_size = MagicMock(bloom.get_size)
    bloom.get_size.return_value = 100

    # Call len
    result = len(bloom)

    # Check the results and calls
    expected_result = 100
    assert expected_result == result
    bloom.get_size.assert_called_once_with()
