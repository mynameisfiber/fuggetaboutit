def assert_bloom_values(bloom, expected_values):
    for key, value in expected_values.iteritems():
        assert value == getattr(bloom, key)