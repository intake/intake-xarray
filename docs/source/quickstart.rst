Quickstart
==========

``intake-netcdf`` provides quick and easy access to stored in NetCDF files.

.. _netcdf: https://www.unidata.ucar.edu/software/netcdf/

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c intake intake-netcdf

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Ad-hoc
~~~~~~

After installation, the function ``intake.open_netcdf``
will become available. It can be used to open netcdf
files, and store the results as an xarray DataSet.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To include in a catalog, the plugin must be listed in the plugins of the catalog::

   plugins:
     source:
       - module: intake_netcdf

and entries must specify ``driver: netcdf``.



Using a Catalog
~~~~~~~~~~~~~~~

