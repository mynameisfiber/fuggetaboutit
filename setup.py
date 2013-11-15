#!/usr/bin/env python2.7
"""
Install script for fuggetaboutit

micha gorelick, mynameisfiber@gmail.com
http://micha.gd/
"""

from setuptools import setup, Extension
import numpy.distutils.misc_util

_optimizations = Extension(
    'fuggetaboutit._optimizations',
    sources = ['fuggetaboutit/_optimizations.c', ],
    extra_compile_args = ["-O3", "-std=c99", "-fopenmp", "-Wall", "-p", "-pg", ],
    extra_link_args = ["-lgomp", "-lc"],
)

setup(
    name = 'fuggetaboutit',
    version = '0.3.2',
    description = 'implementations of a counting bloom filter, a ' \
        'timing bloom filter and a scaling timing bloom filter. ie: bloom ' \
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

    packages = ['fuggetaboutit', 'fuggetaboutit.tests'],
    ext_modules = [_optimizations,],
    include_dirs = numpy.distutils.misc_util.get_numpy_include_dirs(),

    install_requires = [
        "numpy",
        "mmh3",
        "tornado>=3",
    ],
)
