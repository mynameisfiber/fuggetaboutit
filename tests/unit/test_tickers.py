from mock import MagicMock, patch, sentinel
import pytest

tornado_ioloop = pytest.importorskip("tornado.ioloop")

from fuggetaboutit.tickers import TornadoTicker



def test_init__with_io_loop():
    # Get a ticker
    ticker = TornadoTicker(io_loop=sentinel.io_loop)

    # Make sure the ticker is setup as expected
    assert sentinel.io_loop == ticker._io_loop
    assert ticker._callback_timer is None

@patch('tornado.ioloop.IOLoop')
def test_init_without_io_loop(ioloop_mock):
    # Get a ticker
    ticker = TornadoTicker()

    # Make sure the io loop was fetched
    ioloop_mock.current.assert_called_once_with()

    # Make sure the ticker is setup as expected
    assert ioloop_mock.current.return_value == ticker._io_loop
    assert ticker._callback_timer is None
    assert ticker.callback is None
    assert ticker.interval is None

@patch('tornado.ioloop.IOLoop')
@patch('tornado.ioloop.PeriodicCallback')
def test_setup(periodic_callback_mock, ioloop_mock):
    # Get a ticker
    ticker = TornadoTicker()

    # Setup test data
    callback = MagicMock()
    interval = 60

    # Call setup
    ticker.setup(callback, interval)

    # Check that calls were made as expected
    expected_interval = interval * 1000
    periodic_callback_mock.assert_called_once_with(callback, expected_interval, ticker._io_loop)

    # Check that the ticker has the expected state
    assert callback == ticker.callback
    assert interval == ticker.interval
    assert periodic_callback_mock.return_value == ticker._callback_timer

@patch('tornado.ioloop.IOLoop')
def test_setup__redundant(ioloop_mock):
    # Get a ticker
    ticker = TornadoTicker()
    ticker._callback_timer = MagicMock(tornado_ioloop.PeriodicCallback)

    # Call setup and make sure it raises the expected exception
    with pytest.raises(Exception):
        ticker.setup(MagicMock(), 1234)


@patch('tornado.ioloop.IOLoop')
def test_start__no_timer(ioloop_mock):
    # Get a ticker
    ticker = TornadoTicker()

    # Call start and make sure it raises an exception
    with pytest.raises(Exception):
        ticker.start()

@patch('tornado.ioloop.IOLoop')
def test_start__running_timer(ioloop_mock):
    # Get a ticker
    ticker = TornadoTicker()
    ticker._callback_timer = MagicMock(tornado_ioloop.PeriodicCallback)
    ticker._callback_timer._running = True

    # Call start and make sure it raises an exception
    with pytest.raises(Exception):
        ticker.start()

@patch('tornado.ioloop.IOLoop')
def test_start__success(ioloop_mock):
    # Get a ticker
    ticker = TornadoTicker()
    ticker._callback_timer = MagicMock(tornado_ioloop.PeriodicCallback)
    ticker._callback_timer._running = False

    # Call start
    ticker.start()

    # Make sure the timer got started
    ticker._callback_timer.start.assert_called_once_with()

@patch('tornado.ioloop.IOLoop')
def test_stop__no_timer(ioloop_mock):
    # Get a ticker
    ticker = TornadoTicker()

    # Call stop and make sure it raises an exception
    with pytest.raises(Exception):
        ticker.stop()

@patch('tornado.ioloop.IOLoop')
def test_stop__running_timer(ioloop_mock):
    # Get a ticker
    ticker = TornadoTicker()
    ticker._callback_timer = MagicMock(tornado_ioloop.PeriodicCallback)
    ticker._callback_timer._running = False

    # Call stop and make sure it raises an exception
    with pytest.raises(Exception):
        ticker.stop()

@patch('tornado.ioloop.IOLoop')
def test_stop__success(ioloop_mock):
    # Get a ticker
    ticker = TornadoTicker()
    ticker._callback_timer = MagicMock(tornado_ioloop.PeriodicCallback)
    ticker._callback_timer._running = True

    # Call stop
    ticker.stop()

    # Make sure the timer got started
    ticker._callback_timer.stop.assert_called_once_with()
