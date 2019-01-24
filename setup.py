#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from setuptools import setup, find_packages
import versioneer

requires = [line.strip() for line in open(
    'requirements.txt').readlines() if not line.startswith("#")]

setup(
    name='intake-xarray',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='xarray plugins for Intake',
    url='https://github.com/ContinuumIO/intake-xarray',
    maintainer='Julia Signell',
    maintainer_email='jsignell@anaconda.com',
    license='BSD',
    py_modules=['intake_xarray'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    zip_safe=False, )
