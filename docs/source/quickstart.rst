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

After installation, the function ``intake.open_xarray``
will become available. It can be used to open xarray
files, and store the results as an xarray DataSet.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To include in a catalog, the plugin must be listed in the plugins of the catalog::

   plugins:
     source:
       - module: intake_xarray

and entries must specify ``driver: xarray``.



Using a Catalog
~~~~~~~~~~~~~~~

