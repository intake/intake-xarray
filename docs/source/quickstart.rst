Quickstart
==========

``intake-xarray`` provides quick and easy access to n dimensional data
suitable for reading by `xarray`_.

.. _xarray: https://xarray.pydata.org

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c conda-forge intake-xarray

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----


Inline use
~~~~~~~~~~

After installation, the functions ``intake.open_netcdf``,
``intake.open_rasterio``, ``intake.open_zarr``,
``intake.open_xarray_image``, and ``intake.open_opendap`` will become available.
They can be used to open data files as xarray objects.


Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

Catalog entries must specify ``driver: netcdf``, ``driver: rasterio``,
``driver: zarr``, ``driver: xarray_image``, or ``driver: opendap``
as appropriate.


The zarr and image plugins allow access to remote data stores (s3 and gcs),
settings relevant to those should be passed in using the parameter
``storage_options``.


Choosing a Driver
~~~~~~~~~~~~~~~~~

While all the drivers in the ``intake-xarray`` plugin yield ``xarray``
objects, they do not all accept the same file formats.


netcdf/grib
-----------

Supports any local or downloadable file that can be passed to xarray.open_dataset.
Remote files will be cached locally.
This included .nc and .grib but not OPeNDAP URLs as these are not downloadable files.

opendap
-------

Supports OPeNDAP URLs, optionally with `esgf`, `urs` or `generic_http` authentication.

zarr
-----

Supports .zarr directories. See https://zarr.readthedocs.io/ for more
information.

rasterio
--------

Supports any file format supported by ``rasterio.open`` - most commonly
geotiffs.

xarray_image
------------

Supports any file format that can be passed to ``scikit-image.io.imread``
which includes all the common image formats (jpg, png, tif, ...)


