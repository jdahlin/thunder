#!/usr/bin/env python
import os
import re

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup, Extension, find_packages


VERSION = re.search('version = "([^"]+)"',
                    open("thunder/__init__.py").read()).group(1)


setup(
    name="thunder",
    version=VERSION,
    description=(
        "Thunder is an object-relational mapper (ORM) for Python"),
    author="Johan Dahlin",
    author_email="johan@gnome.org",
    license="LGPL",
    url="https://github.com/jdahlin/thunder",
    download_url="https://github.com/jdahlin/thunder/downloads",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        ("License :: OSI Approved :: GNU Library or "
         "Lesser General Public License (LGPL)"),
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    # The following options are specific to setuptools but ignored (with a
    # warning) by distutils.
    include_package_data=True,
    zip_safe=False,
    )
