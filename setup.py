#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from setuptools import setup, find_packages
import versioneer

INSTALL_REQUIRES = ['intake >=2', 'xarray >=02022', 'zarr', 'dask >=2.2', 'netcdf4', 'fsspec>2022',
                    'msgpack', 'requests']

setup(
    name='intake-xarray',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='xarray plugins for Intake',
    url='https://github.com/intake/intake-xarray',
    maintainer='Martin Durant',
    maintainer_email='mdurant@anaconda.com',
    license='BSD',
    py_modules=['intake_xarray'],
    packages=find_packages(),
    entry_points={
        'intake.drivers': [
            'netcdf = intake_xarray.netcdf:NetCDFSource',
            'zarr = intake_xarray.xzarr:ZarrSource',
            'opendap = intake_xarray.opendap:OpenDapSource',
            'xarray_image = intake_xarray.image:ImageSource',
            'rasterio = intake_xarray.raster:RasterIOSource',
        ]
    },
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    zip_safe=False, )
