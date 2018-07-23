Quickstart
==========

``intake-xarray`` provides quick and easy access to stored in xarray files.

.. _xarray: https://xarray.pydata.org

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c intake intake-xarray

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Note that xarray sources do not yet support streaming from an Intake server.

Ad-hoc
~~~~~~

After installation, the functions ``intake.open_netcdf``, ``intake.open_rasterio`` and ``intake.open_zarr``
will become available. They can be used to open xarray
datasets.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

Catalog entries must specify ``driver: netcdf``, ``driver: rasterio`` or ``driver: zarr``,
as appropriate.


The zarr-based plugin allows access to remote data stores (s3 and gcs), settings
relevant to those should be passed in using the parameter ``storage_options``.


Using a Catalog
~~~~~~~~~~~~~~~

