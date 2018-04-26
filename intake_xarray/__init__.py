# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright 2016 Continuum Analytics, Inc.
#
# May be copied and distributed freely only as part of an Anaconda or
# Miniconda installation.
# -----------------------------------------------------------------------------
from intake.source import base
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class NetCDFPlugin(base.Plugin):
    """Plugin for xarray reader"""

    def __init__(self):
        super(NetCDFPlugin, self).__init__(
            name='netcdf',
            version=__version__,
            container='xarray',
            partition_access=True)

    def open(self, urlpath, chunks, **kwargs):
        """
        Create NetCDFSource instance

        Parameters
        ----------
        urlpath: str
            Path to source file(s).
        chunks: int or dict
            Chunks is used to load the new dataset into dask
            arrays. ``chunks={}`` loads the dataset with dask using a single
            chunk for all arrays.
        """
        from intake_xarray.netcdf import NetCDFSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return NetCDFSource(
            urlpath=urlpath,
            chunks=chunks,
            xarray_kwargs=source_kwargs,
            metadata=base_kwargs['metadata'])
