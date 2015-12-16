import time

import pytest

tornado_testing = pytest.importorskip("tornado.testing")

from fuggetaboutit.tickers import TornadoTicker


class TornadoTickerTests(tornado_testing.AsyncTestCase):
    def test_ticker(self):
        # Get a ticker
        ticker = TornadoTicker()

        # Setup the ticker
        self.num_calls = 0
        self.last_call = time.time()
        ticker.setup(callback=self.handle_callback, interval=1)
        ticker.start()

        # Wait for the ticker to tick
        self.wait(timeout=5)

    def handle_callback(self):
        # Make sure it's been atleast 2 seconds
        elapsed_time = time.time() - self.last_call
        assert elapsed_time >= 0.99

        # Update book keeping
        self.num_calls += 1
        self.last_call = time.time()

        # Stop the test on the third call
        if self.num_calls > 2:
            self.stop()
