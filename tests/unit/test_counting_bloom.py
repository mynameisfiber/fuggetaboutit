from copy import copy
import json

from mock import MagicMock, mock_open, patch, sentinel
import numpy as np
import pytest

from fuggetaboutit.counting_bloom_filter import CountingBloomFilter
from fuggetaboutit.exceptions import PersistenceDisabledException

from ..utils import assert_bloom_values


BLOOM_DEFAULTS = {
    'capacity': 1000,
    'error': 0.0002,
    'data_path': '/some/path/',
    'id': 1,
}


def get_bloom(**overrides):
    '''
    Helper function to easily get a bloom for testing.
    '''
    kwargs = copy(BLOOM_DEFAULTS)
    kwargs.update(overrides)

    return CountingBloomFilter(**kwargs)

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
    data_path = '/does/not/exist'
    id = 5

    # Call init and get back a bloom
    bloom = CountingBloomFilter(
        capacity=capacity,
        error=error,
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
        'bloom_filename': '/does/not/exist/bloom.npy',
        'meta_filename': '/does/not/exist/meta.json',
    })

    assert_empty_bloom(bloom)

def test_init_no_data_path():
    # Setup test data
    capacity = 1000
    error = 0.002
    data_path = None
    id = 5

    # Call init and get back a bloom
    bloom = CountingBloomFilter(
        capacity=capacity,
        error=error,
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
        'bloom_filename': None,
        'meta_filename': None,
    })

    assert_empty_bloom(bloom)


@patch('numpy.load')
@patch('os.path.exists')
def test_init_with_bloom_data(exists_mock, load_mock):
    # Setup test data and mocks
    exists_mock.return_value = True
    load_mock.return_value = sentinel.data

    capacity = 1000
    error = 0.002
    data_path = '/does/not/exist'

    # Call init
    bloom = CountingBloomFilter(
        capacity=capacity,
        error=error,
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
        'bloom_filename': '/does/not/exist/bloom.npy',
        'meta_filename': '/does/not/exist/meta.json',
    })

    exists_mock.assert_called_once_with(bloom.bloom_filename)
    load_mock.assert_called_once_with(bloom.bloom_filename)
    expected_bloom_data = sentinel.data
    assert expected_bloom_data == bloom.data


def test_indexes():
    # Get a bloom
    bloom = get_bloom()
    
    # Get the indexes that should get incremented for the item 'test'
    indexes = list(bloom.get_indexes('test'))

    # Check that the returned indexes match the expected results
    expected_indexes = [2461, 16351L, 12513L, 8675L, 4837L, 999L, 14889L, 11051L, 7213L, 3375L, 17265L, 13427L]
    assert expected_indexes == indexes


def test_add():
    # Setup the bloom
    bloom = get_bloom()
    key = 'test'

    # Add a value
    bloom.add(key)

    # Check that the bloom was modified as expected
    expected_non_zero = len(list(bloom.get_indexes(key)))
    assert expected_non_zero == np.count_nonzero(bloom.data)

    # Check that the num_non_zero count was properly incremented
    assert expected_non_zero == bloom.num_non_zero


def test_decrement_empty_bucket():
    # Get a bloom
    bloom = get_bloom()

    # Do the decrement
    bloom.decrement_bucket(index=5)

    # Check that the bloom is unmodified
    assert_empty_bloom(bloom)
    expected_num_non_zero = 0
    assert expected_num_non_zero == bloom.num_non_zero


def test_decrement_to_zero():
    # Setup the bloom
    bloom = get_bloom()
    target_bucket = 999

    # Add a value
    bloom.add('test')

    # Check that the bloom is in the expected state
    assert 1 == bloom.data[target_bucket]
    assert 12 == bloom.num_non_zero

    # Decrement the target bucket
    bloom.decrement_bucket(target_bucket)

    # Check that the decrement did what it was supposed to
    assert 0 == bloom.data[target_bucket]
    assert 11 == bloom.num_non_zero

def test_decrement_greater_than_one():
    # Setup the bloom
    bloom = get_bloom()
    target_bucket = 999

    # Add a value multiple times
    bloom.add('test')
    bloom.add('test')

    # Check that the bloom is in the expected state
    assert 2 == bloom.data[target_bucket]
    assert 12 == bloom.num_non_zero

    # Decrement the target bucket
    bloom.decrement_bucket(target_bucket, N=2)

    # Check that the decrement did what it was supposed to
    assert 0 == bloom.data[target_bucket]
    assert 11 == bloom.num_non_zero


def test_decrement_past_zero():
    # Setup the bloom
    bloom = get_bloom()
    target_bucket = 999

    # Add a value
    bloom.add('test')

    # Check that the bloom is in the expected state
    assert 1 == bloom.data[target_bucket]
    assert 12 == bloom.num_non_zero

    # Decrement the target bucket
    bloom.decrement_bucket(target_bucket, N=2)

    # Check that the decrement did what it was supposed to
    assert 0 == bloom.data[target_bucket]
    assert 11 == bloom.num_non_zero



def test_remove():
    # Setup the bloom
    bloom = get_bloom()
    key = 'test'

    # Add a value and make sure it was added as expected
    bloom.add(key)
    expected_non_zero = len(list(bloom.get_indexes(key)))
    assert expected_non_zero == np.count_nonzero(bloom.data)

    # Remove the same value
    bloom.remove(key)

    # Check that the bloom has been reset back to empty
    assert_empty_bloom(bloom)


def test_noop_remove():
    # Setup the bloom
    bloom = get_bloom()
    key = 'test'

    # Remove the same value
    bloom.remove(key)

    # Check that the bloom is unmodified
    assert_empty_bloom(bloom)


def test_remove_all():
    # Setup the bloom
    bloom = get_bloom()
    key = 'test'

    # Add the key a few times
    bloom.add(key)
    bloom.add(key)

    # Make sure the bloom is in the expected state
    expected_max = 2
    assert expected_max == np.amax(bloom.data)

    # Call remove_all and make sure the bloom is in the expected state
    bloom.remove_all()
    expected_max = 1
    assert expected_max == np.amax(bloom.data)


def test_noop_remove_all():
    # Setup the bloom
    bloom = get_bloom()

    # Call remove_all and make sure the bloom is unmodified
    bloom.remove_all()
    assert_empty_bloom(bloom)


def test_contains_miss():
    # Setup the bloom
    bloom = get_bloom()

    # Add a key
    bloom.add('target')

    # Try to look up a different key
    assert not bloom.contains('miss')


def test_contains_hit():
    # Setup the bloom
    bloom = get_bloom()

    # Add a key
    bloom.add('target')

    # Try to look up the same key
    assert bloom.contains('target')


def test_get_empty_size():
    # Setup the bloom
    bloom = get_bloom()

    # Check that the expected size is returned
    expected_size = 0
    assert expected_size == bloom.get_size()

def test_get_non_empty_size():
    # Setup the bloom
    bloom = get_bloom()

    # Add a few keys
    bloom.add('test1')
    bloom.add('test2')

    # Check that the expected size is returned
    expected_size = 2
    assert expected_size == round(bloom.get_size())


@patch('numpy.save')
def test_flush_data(save_mock):
    # Get a bloom
    bloom = get_bloom()
    bloom.data = sentinel.data

    # Call flush
    bloom.flush_data()

    # Make sure numpy.save was called as expected
    save_mock.assert_called_once_with(bloom.bloom_filename, sentinel.data)


def test_flush_data_without_data_path():
    # Get a bloom
    bloom = get_bloom(data_path=None)

    # Call flush
    with pytest.raises(PersistenceDisabledException):
        bloom.flush_data()

def test_get_meta():
    # Get a bloom
    bloom = get_bloom()

    # Make sure the meta data gets returned as expected
    expected_meta = copy(BLOOM_DEFAULTS)
    del expected_meta['data_path']
    assert expected_meta == bloom.get_meta()


@patch('os.makedirs')
@patch('os.path.exists')
def test_new_save(exists_mock, makedirs_mock):
    # Setup the mocks
    exists_mock.return_value = False
    save_target_mock = mock_open()

    # Get a blooom
    bloom = get_bloom()

    # Mock out the flush call
    bloom.flush_data = MagicMock(bloom.flush_data)

    # Call save
    with patch('__builtin__.open', save_target_mock, create=True):
        bloom.save()

    # Make sure the directory for the bloom got created
    exists_mock.assert_called_with(bloom.data_path)
    makedirs_mock.assert_called_once_with(bloom.data_path)

    # Make sure the bloom got flushed
    bloom.flush_data.assert_called_once_with()

    # Make sure the meta data got saved
    save_target_mock.assert_called_once_with(bloom.meta_filename, 'w')


@patch('os.makedirs')
@patch('os.path.exists')
def test_existing_save(exists_mock, makedirs_mock):
    # Setup the mocks
    exists_mock.side_effect = [False, True]
    save_target_mock = mock_open()

    # Get a blooom
    bloom = get_bloom()

    # Mock out the flush call
    bloom.flush_data = MagicMock(bloom.flush_data)

    # Call save
    with patch('__builtin__.open', save_target_mock, create=True):
        bloom.save()

    # Make sure the directory for the bloom did not get created
    exists_mock.assert_called_with(bloom.data_path)
    assert 0 == makedirs_mock.call_count

    # Make sure the bloom got flushed
    bloom.flush_data.assert_called_once_with()

    # Make sure the meta data got saved
    save_target_mock.assert_called_once_with(bloom.meta_filename, 'w')

def test_savewithout_data_path():
    # Get a bloom
    bloom = get_bloom(data_path=None)

    # Call save
    with pytest.raises(PersistenceDisabledException):
        bloom.save()

@patch('os.path.exists')
def test_load(exists_mock):
    # Setup test data 
    test_data = copy(BLOOM_DEFAULTS)
    del test_data['data_path']
    open_mock = mock_open(read_data=json.dumps(test_data))
    exists_mock.return_value = False
    data_path = '/test/foo/bar'

    # Call load
    with patch('__builtin__.open', open_mock, create=True):
        loaded = CountingBloomFilter.load(data_path)

    # Check that the loaded bloom looks as expected
    for key, value in test_data.iteritems():
        assert value == getattr(loaded, key)

    open_mock.assert_called_with(data_path + '/meta.json', 'r')
    exists_mock.assert_called_once_with(data_path + '/bloom.npy')


def test_contains_magic():
    # Get a bloom
    bloom = get_bloom()

    # Add an item
    key = 'test'
    bloom.add(key)

    # Get it using contains
    assert key in bloom


def test_add_magic():
    # Get a bloom
    bloom = get_bloom()

    # Add an item
    key = 'test'
    bloom += key

    # Check that the item was added
    assert bloom.contains(key)


def test_remove_magic():
    # Get a bloom
    bloom = get_bloom()

    # Add some items
    keeper = 'foo'
    loser = 'bar'
    bloom.add(keeper)
    bloom.add(loser)

    # Remove one item
    bloom -= loser

    # Check that the bloom contains the expected item and not the other
    assert bloom.contains(keeper)
    assert not bloom.contains(loser)


def test_len_magic():
    # Get a bloom
    bloom = get_bloom()

    # Add some items
    bloom.add('test 1')
    bloom.add('test 2')

    # Check that the length returned is as expected
    expected_len = 2
    assert expected_len == round(len(bloom))