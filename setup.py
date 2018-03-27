#!/usr/bin/env python

from setuptools import setup, find_packages
version = '0.0.1'


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-netcdf',
    version=version,
    description='NetCDF plugin for Intake',
    url='https://github.com/ContinuumIO/intake-netcdf',
    maintainer='Mike McCarty',
    maintainer_email='mmccarty@anaconda.com',
    license='BSD',
    py_modules=['intake_netcdf'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
