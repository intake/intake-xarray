#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-xarray',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='xarray plugins for Intake',
    url='https://github.com/ContinuumIO/intake-xarray',
    maintainer='Mike McCarty',
    maintainer_email='mmccarty@anaconda.com',
    license='BSD',
    py_modules=['intake_xarray'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
