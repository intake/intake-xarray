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

Ad-hoc
~~~~~~

After installation, the functions ``intake.open_netcdf`` and ``intake.open_zarr``
will become available. They can be used to open xarray
datasets.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To include in a catalog, the plugin must be listed in the plugins of the catalog::

   plugins:
     source:
       - module: intake_xarray

and entries must specify ``driver: netcdf`` or ``driver: zarr``.


The zarr-based plugin allows access to remote data stores (s3 and gcs), settings
relevant to those should be passed in using the parameter ``storage_options``.


Using a Catalog
~~~~~~~~~~~~~~~

