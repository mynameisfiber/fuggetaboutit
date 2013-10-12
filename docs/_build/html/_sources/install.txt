Installing
==========

pypi
####

To install the latest stable version with pypi, simply run:
::

    $ pip install fuggetaboutit

This will automatically install ``fuggetaboutit`` and all it's dependencies

source
######

First, fetch the source:
::

    $ git clone git@github.com:mynameisfiber/fuggetaboutit.git
    $ cd fuggetaboutit

Next, install the dependencies:
::

    $ [sudo] pip install -r requirements.txt

And finally, install ``fuggetaboutit``:
::

    $ [sudo] python setup.py install

testing
#######

Once installed, tests can be run with the following command:
::

    $ python -m tornado.testing fuggetaboutit.test
