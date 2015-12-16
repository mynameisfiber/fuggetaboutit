from copy import copy

from mock import patch, sentinel
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
    data_path = '/does/not/exist/'
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
        'id': id,
        'num_bytes': 12935,
        'num_hashes': 9,
    })

    test_dp, test_mf, test_bf = bloom._get_paths(None)
    assert test_dp == '/does/not/exist'
    assert test_mf == '/does/not/exist/meta.json'
    assert test_bf == '/does/not/exist/bloom.npy'

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
        'data_path': None,
        'id': id,
        'num_bytes': 12935,
        'num_hashes': 9,
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
        'data_path': '/does/not/exist',
        'id': None,
        'num_bytes': 12935,
        'num_hashes': 9,
    })

    _, _, bloom_filename = bloom._get_paths(None)
    exists_mock.assert_called_once_with(bloom_filename)
    load_mock.assert_called_once_with(bloom_filename)

    assert sentinel.data == bloom.data


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


def test_flush_data__without_data_path():
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


# TODO add more tests for saving and loading involving existing files and directories


def test_save__without_data_path():
    # Get a bloom
    bloom = get_bloom(data_path=None)

    # Call save
    with pytest.raises(PersistenceDisabledException):
        bloom.save()


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
