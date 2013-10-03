#!/usr/bin/env python2.7
"""
Install script for fuggetaboutit

micha gorelick, mynameisfiber@gmail.com
http://micha.gd/
"""

from setuptools import setup

setup(
    name = 'fuggetaboutit',
    version = '0.1.0',
    description = 'pure python implementations of a counting bloom filter, a' \
        'timing bloom filter and a scaling timing bloom filter. ie: bloom' \
        'filters for the stream',
    author = 'Micha Gorelick',
    author_email = 'mynameisfiber@gmail.com',
    url = 'http://github.com/mynameisfiber/fuggetaboutit/',
    download_url = 'https://github.com/mynameisfiber/fuggetaboutit/tarball/master',
    license = "GNU Lesser General Public License v3 or later (LGPLv3+)",

    classifiers = [
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
    ],

    packages = ['fuggetaboutit',],

    install_requires = [
        "mmh3",
        "tornado>=3",
    ],
)
