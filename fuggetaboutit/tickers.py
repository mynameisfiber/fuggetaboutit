import logging


class NoOpTicker(object):
    '''
    Ticker implementation that no-ops instead of calling back for testing purposes.
    '''
    def setup(self, callback, interval):
        pass

    def start(self):
        pass

    def stop(self):
        pass


class TornadoTicker(object):
    '''
    Ticker implementation that uses Tornado's IO loop to perform periodic callbacks.
    '''
    
    def __init__(self, io_loop=None):
        try:
            import tornado.ioloop
        except ImportError:
            logging.exception("Tornado must be installed to use the TornadoTicker")
            raise
        
        self._io_loop = io_loop or tornado.ioloop.IOLoop.current()
        self._callback_timer = None
        self.callback = None
        self.interval = None

        super(TornadoTicker, self).__init__()

    def setup(self, callback, interval):
        try:
            import tornado.ioloop
        except ImportError:
            logging.exception("Tornado must be installed to use the TornadoTicker")
            raise

        if self._callback_timer:
            raise Exception("Ticker already setup")

        self.callback = callback
        self.interval = interval

        self._callback_timer = tornado.ioloop.PeriodicCallback(callback, interval * 1000, self._io_loop)

    def start(self):
        if not self._callback_timer:
            raise Exception("You need to call the setup method before calling start.")

        if self._callback_timer._running:
            raise Exception("Can't start an already running timer.")

        self._callback_timer.start()

    def stop(self):
        if not self._callback_timer:
            raise Exception("You need to call the setup method before calling stop.")

        if not self._callback_timer._running:
            raise Exception("Can't stop a timer that isn't running.")

        self._callback_timer.stop()